# -----------------------------------------------------------------------------
# Servidor gRPC de DataNode: almacenamiento seguro de bloques, staging .part, capabilities e integridad.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import os
import sys
from pathlib import Path
import re
import hashlib
import time
from concurrent import futures

import grpc

import datanode_pb2
import datanode_pb2_grpc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from shared.grpc_tls import server_credentials
from shared.capability import (
    token_from_context,
    validate_block_capability,
)


DATA_DIR = os.environ.get(
    "DFSHA_DATA_DIR",
    os.path.expanduser("~/dfsha/storage/d1/blocks")
)

PORT = os.environ.get("DFSHA_DATANODE_PORT", "50051")
NODE_ID = os.environ.get("DFSHA_DATANODE_ID", "d1").lower()

BLOCK_ID_PATTERN = re.compile(r"^[A-Za-z0-9._-]{1,200}$")


# Valida el identificador de bloque antes de convertirlo en una ruta local.
def validate_block_id(block_id: str):
    if not block_id or not BLOCK_ID_PATTERN.fullmatch(block_id):
        raise ValueError("block_id inválido")


# Obtiene la ruta local segura asociada a un block_id validado.
def block_path(block_id: str):
    validate_block_id(block_id)
    return os.path.join(DATA_DIR, block_id)


# Implementación gRPC de las operaciones que ofrece cada DataNode.
class DataNodeService(datanode_pb2_grpc.DataNodeServicer):

    # Recibe un bloque autorizado, lo escribe primero en staging y lo publica de forma atómica.
    def PutBlock(self, request_iterator, context):
        os.makedirs(DATA_DIR, exist_ok=True)

        block_id = None
        temp_path = None
        final_path = None
        total_size = 0
        hasher = hashlib.sha256()

        try:
            capability_token = token_from_context(context)
        except Exception as exc:
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            context.set_details(f"Capacidad PUT ausente o inválida: {exc}")
            return datanode_pb2.BlockAck(
                success=False,
                block_id="",
                message="Acceso denegado",
            )

        try:
            for chunk in request_iterator:
                if block_id is None:
                    block_id = chunk.block_id
                    validate_block_id(block_id)

                    try:
                        validate_block_capability(
                            capability_token,
                            block_id=block_id,
                            operation="put",
                            node_id=NODE_ID,
                        )
                    except Exception as exc:
                        context.set_code(grpc.StatusCode.PERMISSION_DENIED)
                        context.set_details(
                            f"Capacidad PUT inválida: {exc}"
                        )
                        return datanode_pb2.BlockAck(
                            success=False,
                            block_id=block_id,
                            message="Acceso denegado",
                        )

                    final_path = block_path(block_id)
                    temp_path = final_path + ".part"

                    file_obj = open(temp_path, "wb")

                elif chunk.block_id != block_id:
                    raise ValueError(
                        "Todos los chunks deben pertenecer al mismo bloque"
                    )

                file_obj.write(chunk.data)
                hasher.update(chunk.data)
                total_size += len(chunk.data)

            if block_id is None:
                raise ValueError("No se recibió ningún bloque")

            file_obj.flush()
            os.fsync(file_obj.fileno())
            file_obj.close()

            os.replace(temp_path, final_path)

            return datanode_pb2.BlockAck(
                success=True,
                block_id=block_id,
                sha256=hasher.hexdigest(),
                size=total_size,
                message="Bloque almacenado correctamente"
            )

        except Exception as exc:
            try:
                if "file_obj" in locals() and not file_obj.closed:
                    file_obj.close()
            except Exception:
                pass

            if temp_path and os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except OSError:
                    pass

            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(exc))

            return datanode_pb2.BlockAck(
                success=False,
                block_id=block_id or "",
                message=str(exc)
            )

    # Entrega un bloque únicamente si la capability GET y el canal mTLS son válidos.
    def GetBlock(self, request, context):
        try:
            validate_block_id(request.block_id)
        except ValueError as exc:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(exc),
            )

        try:
            capability_token = token_from_context(context)
            validate_block_capability(
                capability_token,
                block_id=request.block_id,
                operation="get",
                node_id=NODE_ID,
            )
        except Exception as exc:
            context.abort(
                grpc.StatusCode.PERMISSION_DENIED,
                f"Capacidad GET ausente o inválida: {exc}",
            )

        try:
            path = block_path(request.block_id)

            if not os.path.isfile(path):
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Bloque no encontrado"
                )

            chunk_size = 256 * 1024

            with open(path, "rb") as file_obj:
                while True:
                    data = file_obj.read(chunk_size)

                    if not data:
                        break

                    yield datanode_pb2.BlockChunk(
                        block_id=request.block_id,
                        data=data
                    )

        except ValueError as exc:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(exc)
            )

    # Devuelve tamaño/hash del bloque autorizado sin modificar su contenido.
    def StatBlock(self, request, context):
        try:
            path = block_path(request.block_id)

            if not os.path.isfile(path):
                context.abort(
                    grpc.StatusCode.NOT_FOUND,
                    "Bloque no encontrado"
                )

            hasher = hashlib.sha256()
            size = 0

            with open(path, "rb") as file_obj:
                while True:
                    data = file_obj.read(256 * 1024)

                    if not data:
                        break

                    hasher.update(data)
                    size += len(data)

            return datanode_pb2.BlockAck(
                success=True,
                block_id=request.block_id,
                sha256=hasher.hexdigest(),
                size=size,
                message="Bloque disponible"
            )

        except ValueError as exc:
            context.abort(
                grpc.StatusCode.INVALID_ARGUMENT,
                str(exc)
            )



PART_TTL_SECONDS = int(
    os.environ.get("DFSHA_PART_TTL_SECONDS", "300")
)


# Elimina archivos .part abandonados cuyo TTL ya expiró.
def cleanup_stale_parts():
    """Elimina temporales .part abandonados por escrituras interrumpidas."""
    now = time.time()
    removed = 0

    for name in os.listdir(DATA_DIR):
        if not name.endswith(".part"):
            continue

        path = os.path.join(DATA_DIR, name)

        try:
            age = now - os.path.getmtime(path)

            if age >= PART_TTL_SECONDS:
                os.remove(path)
                removed += 1
                print(f"Temporal eliminado: {name}")

        except OSError as exc:
            print(f"No se pudo limpiar {name}: {exc}")

    return removed

# Inicializa el DataNode, limpia staging antiguo y levanta el servidor gRPC con mTLS.
def serve():
    os.makedirs(DATA_DIR, exist_ok=True)

    removed = cleanup_stale_parts()
    print(
        f"Limpieza staging: {removed} temporal(es) eliminado(s) "
        f"| TTL={PART_TTL_SECONDS}s"
    )

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    datanode_pb2_grpc.add_DataNodeServicer_to_server(
        DataNodeService(),
        server
    )

    bound_port = server.add_secure_port(
        f"0.0.0.0:{PORT}",
        server_credentials(NODE_ID),
    )

    if bound_port == 0:
        raise RuntimeError(
            f"No fue posible abrir el puerto gRPC seguro {PORT}"
        )
    server.start()

    print(
        f"DFSha DataNode {NODE_ID.upper()} escuchando "
        f"con mTLS en puerto {PORT}"
    )
    print("Autenticación mutua TLS: requerida")
    print(f"Directorio de bloques: {DATA_DIR}")

    server.wait_for_termination()


if __name__ == "__main__":
    serve()
