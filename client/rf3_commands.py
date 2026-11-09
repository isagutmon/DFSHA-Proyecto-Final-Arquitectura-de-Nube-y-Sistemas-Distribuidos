# -----------------------------------------------------------------------------
# Comandos RF3 del cliente: handles, lectura por rango, lock y escritura Copy-on-Write.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import hashlib
import json
import os
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
                timeout=20,
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

        typer.echo(
            f"ERROR {response.status_code}: {message}",
            err=True,
        )
        raise typer.Exit(1)

    return response


# Obtiene el handle RF3 activo guardado en la sesión del cliente.
def active_handle():
    session = load_session()
    handle_id = session.get("handle_id")

    if not handle_id:
        typer.echo(
            "ERROR: no hay archivo abierto. Use open.",
            err=True,
        )
        raise typer.Exit(1)

    return handle_id


# Descarga un bloque desde una réplica autorizada y valida el resultado recibido.
def fetch_block(block):
    for replica in block.get("replicas", []):
        try:
            channel = secure_channel(replica["endpoint"])
            stub = datanode_pb2_grpc.DataNodeStub(channel)
            data = bytearray()

            for chunk in stub.GetBlock(
                datanode_pb2.BlockRequest(
                    block_id=block["block_id"]
                ),
                timeout=20,
                  metadata=(
                      ("x-dfsha-capability", replica["capability"]),
                  ),
            ):
                data.extend(chunk.data)

            result = bytes(data)

            if (
                len(result) == block["size"]
                and hashlib.sha256(result).hexdigest()
                == block["sha256"]
            ):
                return result, replica["node_id"]

        except grpc.RpcError:
            continue

    raise RuntimeError(
        f"No se pudo recuperar bloque {block['block_id']}"
    )


# Envía un bloque a un DataNode usando el canal seguro y la capability correspondiente.
def put_block(block_id, data, replica):
    channel = secure_channel(replica["endpoint"])
    stub = datanode_pb2_grpc.DataNodeStub(channel)

    # Genera el stream de mensajes gRPC utilizado en una transferencia de bloque.
    def stream():
        for pos in range(0, len(data), RPC_CHUNK):
            yield datanode_pb2.BlockChunk(
                block_id=block_id,
                data=data[pos:pos + RPC_CHUNK],
            )

    return stub.PutBlock(
        stream(),
        timeout=20,
        metadata=(
            ("x-dfsha-capability", replica["capability"]),
        ),
    )


# Solicita el manifest vigente del archivo al plano de control.
def get_manifest(path):
    response = request(
        "GET",
        "/v1/files/manifest",
        params={"path": path},
    )
    require_ok(response)
    return response.json()


# Registra en Typer los comandos RF3 del cliente.
def register_rf3_commands(app):

    # Abre un archivo existente y conserva un handle asociado a una versión concreta.
    @app.command("open")
    def open_cmd(
        path: str = typer.Argument(...),
        mode: str = typer.Argument("r"),
    ):
        """Abrir archivo existente en modo r o rw."""

        response = request(
            "POST",
            "/v1/files/open",
            json={"path": path, "mode": mode},
        )
        require_ok(response)
        handle = response.json()

        session = load_session()
        session["handle_id"] = handle["handle_id"]
        session["open_path"] = handle["path"]
        session["open_mode"] = handle["mode"]
        save_session(session)

        typer.echo(
            f"OPEN OK: {handle['path']} | "
            f"modo={handle['mode']} | "
            f"versión={handle['version']}"
        )
        typer.echo(f"HANDLE: {handle['handle_id']}")

    # Solicita el lock exclusivo de escritura para el handle activo.
    @app.command("lock")
    def lock_cmd():
        """Adquirir lock exclusivo para el archivo abierto."""

        handle_id = active_handle()

        response = request(
            "POST",
            "/v1/files/lock",
            json={"handle_id": handle_id},
        )
        require_ok(response)

        typer.echo(
            f"LOCK OK | TTL={response.json()['ttl']}s"
        )

    # Renueva el lease del lock de escritura antes de su expiración.
    @app.command("renew-lock")
    def renew_lock_cmd():
        """Renovar el lock exclusivo."""

        handle_id = active_handle()

        response = request(
            "POST",
            "/v1/files/lock/renew",
            json={"handle_id": handle_id},
        )
        require_ok(response)

        typer.echo(
            f"LOCK RENOVADO | TTL={response.json()['ttl']}s"
        )

    # Ejecuta una lectura por rango utilizando el plan de bloques entregado por el Control.
    @app.command("read")
    def read_cmd(
        offset: int = typer.Argument(...),
        length: int = typer.Argument(...),
        output: str = typer.Option("", "--output"),
    ):
        """Leer un rango del archivo abierto."""

        handle_id = active_handle()

        response = request(
            "POST",
            "/v1/files/read-plan",
            json={
                "handle_id": handle_id,
                "offset": offset,
                "length": length,
            },
        )
        require_ok(response)
        plan = response.json()

        result = bytearray(plan["actual_length"])

        for segment in plan["segments"]:
            block = {
                "block_id": segment["block_id"],
                "size": segment["size"],
                "sha256": segment["sha256"],
                "replicas": segment["replicas"],
            }

            data, node = fetch_block(block)

            start = segment["block_offset"]
            stop = start + segment["length"]
            out_start = segment["output_offset"]
            out_stop = out_start + segment["length"]

            result[out_start:out_stop] = data[start:stop]

            typer.echo(
                f"READ bloque {segment['index']} <- {node} | "
                f"{segment['length']} bytes"
            )

        if output:
            Path(output).write_bytes(result)
            typer.echo(
                f"READ OK: offset={offset} | "
                f"bytes={len(result)} -> {output}"
            )
        else:
            try:
                typer.echo(result.decode("utf-8"))
            except UnicodeDecodeError:
                typer.echo(result.hex())

    # Ejecuta una escritura parcial mediante Copy-on-Write sobre los bloques afectados.
    @app.command("write")
    def write_cmd(
        offset: int = typer.Argument(...),
        patch_file: str = typer.Argument(...),
    ):
        """Modificar bytes existentes sin extender el archivo."""

        handle_id = active_handle()
        patch_path = Path(patch_file)

        if not patch_path.is_file():
            typer.echo("ERROR: patch local no encontrado", err=True)
            raise typer.Exit(1)

        patch = patch_path.read_bytes()
        patch_hash = hashlib.sha256(patch).hexdigest()

        response = request(
            "POST",
            "/v1/files/write-plan",
            json={
                "handle_id": handle_id,
                "offset": offset,
                "size": len(patch),
                "patch_sha256": patch_hash,
            },
        )
        require_ok(response)
        plan = response.json()

        changed = {}
        confirmations = []

        for item in plan["affected"]:
            old_block = {
                "block_id": item["old_block_id"],
                "size": item["old_size"],
                "sha256": item["old_sha256"],
                "replicas": item["old_replicas"],
            }

            original, source_node = fetch_block(old_block)
            modified = bytearray(original)

            p_start = item["patch_offset"]
            p_stop = p_start + item["patch_length"]
            b_start = item["block_offset"]
            b_stop = b_start + item["patch_length"]

            modified[b_start:b_stop] = patch[p_start:p_stop]
            modified = bytes(modified)
            modified_hash = hashlib.sha256(modified).hexdigest()

            stored = []

            for replica in item["new_replicas"]:
                ack = put_block(
                    item["new_block_id"],
                    modified,
                    replica,
                )

                if (
                    not ack.success
                    or ack.size != len(modified)
                    or ack.sha256 != modified_hash
                ):
                    typer.echo(
                        f"ERROR: réplica inválida {replica['node_id']}",
                        err=True,
                    )
                    raise typer.Exit(1)

                stored.append(replica["node_id"])

            changed[item["index"]] = modified
            confirmations.append({
                "index": item["index"],
                "size": len(modified),
                "sha256": modified_hash,
            })

            typer.echo(
                f"COW bloque {item['index']} | "
                f"origen={source_node} | "
                f"nuevo->{','.join(stored)}"
            )

        session = load_session()
        open_path = session["open_path"]
        manifest = get_manifest(open_path)

        whole_hash = hashlib.sha256()

        for block in sorted(
            manifest["blocks"],
            key=lambda x: x["index"],
        ):
            if block["index"] in changed:
                data = changed[block["index"]]
            else:
                data, _ = fetch_block(block)

            whole_hash.update(data)

        commit = request(
            "POST",
            f"/v1/writes/{plan['operation_id']}/commit",
            json={
                "handle_id": handle_id,
                "blocks": confirmations,
                "file_sha256": whole_hash.hexdigest(),
            },
        )
        require_ok(commit)
        result = commit.json()

        typer.echo(
            f"WRITE COMMIT OK | versión={result['version']} | "
            f"bloques modificados={result['changed_blocks']}"
        )
        typer.echo(f"SHA-256: {result['sha256']}")

    # Cierra el handle activo y libera el lock si pertenece al cliente.
    @app.command("close")
    def close_cmd():
        """Cerrar el handle activo y liberar su lock."""

        handle_id = active_handle()

        response = request(
            "POST",
            "/v1/files/close",
            json={"handle_id": handle_id},
        )
        require_ok(response)

        session = load_session()
        session.pop("handle_id", None)
        session.pop("open_path", None)
        session.pop("open_mode", None)
        save_session(session)

        typer.echo(
            f"CLOSE OK: {response.json()['path']}"
        )
