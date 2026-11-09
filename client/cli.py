# -----------------------------------------------------------------------------
# Cliente CLI principal: autenticación, sesión y operaciones RF1 sobre el namespace lógico de DFSha.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import json
import os
import posixpath
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import httpx
import typer

app = typer.Typer(help="Cliente CLI de DFSha")

CONTROLS_RAW = os.getenv("DFSHA_CONTROLS", "").strip()
if not CONTROLS_RAW:
    raise RuntimeError(
        "Define DFSHA_CONTROLS con las URL HTTPS de Control A/B"
    )

CONTROLS = [
    control.strip().rstrip("/")
    for control in CONTROLS_RAW.split(",")
    if control.strip()
]

CA_CERT = os.getenv(
    "DFSHA_CA_CERT",
    str(Path(__file__).resolve().parents[1] / "certs" / "ca.crt"),
)

SESSION_FILE = Path.home() / ".dfsha" / "session.json"

# Carga la sesión persistida del cliente, si existe.
def load_session():
    if not SESSION_FILE.exists():
        return {"token": None, "cwd": "/"}
    return json.loads(SESSION_FILE.read_text())

# Persiste de forma local el estado de sesión del cliente.
def save_session(session):
    SESSION_FILE.parent.mkdir(parents=True, exist_ok=True)
    SESSION_FILE.write_text(json.dumps(session, indent=2))

# Centraliza las solicitudes HTTPS al plano de control y el failover entre controles configurados.
def request(method, path, **kwargs):
    session = load_session()
    headers = kwargs.pop("headers", {})
    if session.get("token"):
        headers["Authorization"] = f"Bearer {session['token']}"
    last_error = None
    for control in CONTROLS:
        try:
            return httpx.request(
                method,
                control + path,
                headers=headers,
                verify=CA_CERT,
                timeout=15,
                **kwargs
            )
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"No hay Control A/B disponible: {last_error}")

# Resuelve una ruta relativa respecto al directorio lógico actual del cliente.
def resolve_path(path):
    session = load_session()
    cwd = session.get("cwd", "/")
    if path.startswith("/"):
        result = posixpath.normpath(path)
    else:
        result = posixpath.normpath(posixpath.join(cwd, path))
    if not result.startswith("/"):
        result = "/" + result
    return result

# Valida la respuesta HTTP y detiene la operación si el Control reporta un error.
def require_ok(response):
    if response.status_code >= 400:
        try:
            message = response.json().get("detail", response.text)
        except Exception:
            message = response.text
        typer.echo(f"ERROR {response.status_code}: {message}", err=True)
        raise typer.Exit(1)
    return response

# Registra un usuario en el plano de control.
@app.command()
def register(
    username: str = typer.Argument(...),
    password: str = typer.Argument(...),
):
    r = request(
        "POST",
        "/v1/auth/register",
        json={"username": username, "password": password},
    )
    require_ok(r)
    typer.echo(f"Usuario creado: {username}")

# Autentica al usuario y obtiene la sesión/JWT que usará el cliente.
@app.command()
def login(
    username: str = typer.Argument(...),
    password: str = typer.Argument(...),
):
    r = request(
        "POST",
        "/v1/auth/login",
        json={"username": username, "password": password},
    )
    require_ok(r)
    save_session({
        "token": r.json()["access_token"],
        "username": username,
        "cwd": "/",
    })
    typer.echo(f"Autenticado como {username}")

# Muestra el directorio lógico actual.
@app.command()
def pwd():
    typer.echo(load_session().get("cwd", "/"))

# Lista entradas del namespace lógico.
@app.command()
def ls(path: str = typer.Argument(".")):
    target = resolve_path(path)
    r = request("GET", "/v1/fs/ls", params={"path": target})
    require_ok(r)
    entries = r.json()["entries"]
    if not entries:
        typer.echo("(vacío)")
        return
    for item in entries:
        name = posixpath.basename(item["path"])
        kind = "DIR" if item["type"] == "dir" else "FILE"
        typer.echo(f"{kind:4}  {name}")

# Cambia el directorio lógico actual del cliente.
@app.command()
def cd(path: str = typer.Argument(...)):
    target = resolve_path(path)
    r = request("GET", "/v1/fs/stat", params={"path": target})
    require_ok(r)
    if r.json()["type"] != "dir":
        typer.echo("ERROR: la ruta no es un directorio")
        raise typer.Exit(1)
    session = load_session()
    session["cwd"] = target
    save_session(session)
    typer.echo(target)

# Crea un directorio en el namespace lógico.
@app.command()
def mkdir(path: str = typer.Argument(...)):
    target = resolve_path(path)
    r = request("POST", "/v1/fs/mkdir", json={"path": target})
    require_ok(r)
    typer.echo(f"Directorio creado: {target}")

# Elimina un directorio vacío del namespace lógico.
@app.command()
def rmdir(path: str = typer.Argument(...)):
    target = resolve_path(path)
    r = request("DELETE", "/v1/fs/rmdir", params={"path": target})
    require_ok(r)
    typer.echo(f"Directorio eliminado: {target}")

# Elimina un archivo del namespace lógico.
@app.command()
def rm(path: str = typer.Argument(...)):
    target = resolve_path(path)
    r = request("DELETE", "/v1/fs/rm", params={"path": target})
    require_ok(r)
    typer.echo(f"Archivo eliminado: {target}")

# Elimina la sesión local del cliente.
@app.command()
def logout():
    if SESSION_FILE.exists():
        SESSION_FILE.unlink()
    typer.echo("Sesión cerrada")

from rf2_commands import register_rf2_commands
register_rf2_commands(app)

from rf3_commands import register_rf3_commands
register_rf3_commands(app)

if __name__ == "__main__":
    app()
