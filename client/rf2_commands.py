# -----------------------------------------------------------------------------
# Comandos RF2 del cliente: carga/descarga de archivos completos, particionamiento e integridad SHA-256.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import hashlib
import json
import os
import posixpath
import sys
from pathlib import Path

import grpc
import httpx
import typer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATANODE_DIR = os.path.join(PROJECT_ROOT, "datanode")
if DATANODE_DIR not in sys.path:
    sys.path.insert(0, DATANODE_DIR)

import datanode_pb2
import datanode_pb2_grpc
from shared.grpc_tls import secure_channel

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
RPC_CHUNK = 256 * 1024

# Carga la sesión persistida del cliente, si existe.
def load_session():
    if not SESSION_FILE.exists():
        return {"token": None, "cwd": "/"}
    return json.loads(SESSION_FILE.read_text())

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
                **kwargs,
            )
        except Exception as exc:
            last_error = exc
    raise RuntimeError(f"No hay Control A/B disponible: {last_error}")

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

# Calcula SHA-256 del archivo local para comprobar integridad extremo a extremo.
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(1024 * 1024)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

# Genera los fragmentos que se envían al DataNode durante una escritura gRPC.
def chunk_stream(block_id, data):
    for pos in range(0, len(data), RPC_CHUNK):
        yield datanode_pb2.BlockChunk(
            block_id=block_id,
            data=data[pos:pos + RPC_CHUNK],
        )

# Registra en Typer los comandos RF2 del cliente.
def register_rf2_commands(app):

    # Sube datos/bloques al sistema siguiendo el plan emitido por el Control.
    @app.command()
    def put(
        local_path: str = typer.Argument(...),
        remote_path: str = typer.Argument(...),
    ):
        local = Path(local_path)
        if not local.is_file():
            typer.echo("ERROR: archivo local no encontrado", err=True)
            raise typer.Exit(1)

        target = resolve_path(remote_path)
        file_size = local.stat().st_size
        file_hash = sha256_file(local)

        start = request(
            "POST",
            "/v1/uploads/start",
            json={"path": target, "size": file_size, "sha256": file_hash},
        )
        require_ok(start)
        plan = start.json()
        confirmations = []

        with open(local, "rb") as source:
            for block in sorted(plan["blocks"], key=lambda item: item["index"]):
                data = source.read(block["size"])
                if len(data) != block["size"]:
                    typer.echo(
                        f"ERROR: lectura incompleta del bloque {block['index']}",
                        err=True,
                    )
                    raise typer.Exit(1)

                block_hash = hashlib.sha256(data).hexdigest()
                stored_on = []

                for replica in block["replicas"]:
                    try:
                        channel = secure_channel(replica["endpoint"])
                        stub = datanode_pb2_grpc.DataNodeStub(channel)
                        ack = stub.PutBlock(
                            chunk_stream(block["block_id"], data),
                            timeout=20,
                            metadata=(
                                ("x-dfsha-capability", replica["capability"]),
                            ),
                        )
                    except grpc.RpcError as exc:
                        typer.echo(
                            f"ERROR {replica['node_id']}: {exc.code().name}",
                            err=True,
                        )
                        raise typer.Exit(1)

                    if (
                        not ack.success
                        or ack.sha256 != block_hash
                        or ack.size != len(data)
                    ):
                        typer.echo(
                            f"ERROR: confirmación inválida de {replica['node_id']}",
                            err=True,
                        )
                        raise typer.Exit(1)

                    stored_on.append(replica["node_id"])

                confirmations.append({
                    "index": block["index"],
                    "size": len(data),
                    "sha256": block_hash,
                })

                typer.echo(
                    f"Bloque {block['index']} ({len(data)} bytes) -> "
                    f"{','.join(stored_on)}"
                )

        commit = request(
            "POST",
            f"/v1/uploads/{plan['operation_id']}/commit",
            json={"blocks": confirmations},
        )
        require_ok(commit)
        result = commit.json()

        typer.echo(
            f"PUT OK: {target} | {result['blocks']} bloques | "
            f"R={result['replication_factor']}"
        )
        typer.echo(f"SHA-256: {file_hash}")

    # Recupera datos/bloques y reconstruye el archivo solicitado.
    @app.command()
    def get(
        remote_path: str = typer.Argument(...),
        local_path: str = typer.Argument(...),
    ):
        target = resolve_path(remote_path)

        manifest_response = request(
            "GET",
            "/v1/files/manifest",
            params={"path": target},
        )
        require_ok(manifest_response)
        manifest = manifest_response.json()
        output = Path(local_path)

        with open(output, "wb") as destination:
            for block in sorted(manifest["blocks"], key=lambda item: item["index"]):
                recovered = None
                source_node = None

                for replica in block.get("replicas", []):
                    try:
                        channel = secure_channel(replica["endpoint"])
                        stub = datanode_pb2_grpc.DataNodeStub(channel)
                        data = bytearray()

                        for chunk in stub.GetBlock(
                            datanode_pb2.BlockRequest(block_id=block["block_id"]),
                            timeout=20,
                            metadata=(
                                ("x-dfsha-capability", replica["capability"]),
                            ),
                        ):
                            data.extend(chunk.data)

                        candidate = bytes(data)

                        if (
                            len(candidate) == block["size"]
                            and hashlib.sha256(candidate).hexdigest()
                            == block["sha256"]
                        ):
                            recovered = candidate
                            source_node = replica["node_id"]
                            break
                    except grpc.RpcError:
                        continue

                if recovered is None:
                    typer.echo(
                        f"ERROR: bloque {block['index']} no recuperable",
                        err=True,
                    )
                    raise typer.Exit(1)

                destination.write(recovered)
                typer.echo(f"Bloque {block['index']} <- {source_node}")

        recovered_hash = sha256_file(output)

        if recovered_hash != manifest["sha256"]:
            typer.echo("ERROR: SHA-256 final no coincide", err=True)
            raise typer.Exit(1)

        typer.echo(
            f"GET OK: {target} -> {output} | {len(manifest['blocks'])} bloques"
        )
        typer.echo(f"SHA-256: {recovered_hash}")
