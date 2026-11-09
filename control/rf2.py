# -----------------------------------------------------------------------------
# Endpoints RF2 del plano de control: planificación, verificación de réplicas y publicación de manifests.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import json
import math
import os
import sys
import uuid

import grpc
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from metadata.etcd_store import EtcdStore
from metadata.fs_store import FSStore
from shared.config import BLOCK_SIZE, REPLICATION_FACTOR
from shared.security import decode_token
from shared.capability import create_block_capability


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATANODE_DIR = os.path.join(PROJECT_ROOT, "datanode")
if DATANODE_DIR not in sys.path:
    sys.path.insert(0, DATANODE_DIR)

import datanode_pb2
import datanode_pb2_grpc
from shared.grpc_tls import secure_channel


router = APIRouter()
bearer = HTTPBearer(auto_error=False)

etcd = EtcdStore()
fs = FSStore()


# Modelo de entrada para iniciar una carga RF2.
class UploadStartRequest(BaseModel):
    path: str
    size: int = Field(ge=0)
    sha256: str = Field(min_length=64, max_length=64)


# Modelo que describe un bloque confirmado y sus réplicas.
class BlockCommit(BaseModel):
    index: int
    size: int = Field(ge=0)
    sha256: str = Field(min_length=64, max_length=64)


# Modelo de entrada para confirmar/publicar una carga RF2.
class UploadCommitRequest(BaseModel):
    blocks: list[BlockCommit]


# Extrae y valida la identidad del usuario desde el JWT de la petición.
def current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer),
):
    if not credentials:
        raise HTTPException(status_code=401, detail="Token requerido")

    try:
        payload = decode_token(credentials.credentials)
        return {
            "user_id": payload["sub"],
            "username": payload["username"],
        }
    except Exception:
        raise HTTPException(
            status_code=401,
            detail="Token inválido o expirado",
        )


# Comprueba rápidamente si el endpoint de un DataNode es alcanzable.
def node_is_reachable(endpoint: str, timeout: float = 1.5) -> bool:
    channel = secure_channel(endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout)
        return True
    except grpc.FutureTimeoutError:
        return False
    finally:
        channel.close()


# Obtiene los DataNodes registrados que están disponibles para participar en una operación.
def active_nodes():
    nodes = []

    for _, raw in etcd.get_prefix("/dfsha/v1/nodes/"):
        node = json.loads(raw)

        if (
            node.get("status") == "active"
            and node_is_reachable(node["endpoint"])
        ):
            nodes.append(node)

    return sorted(nodes, key=lambda n: n["node_id"])


# Consulta de forma segura el estado de un bloque almacenado en un DataNode.
def stat_remote_block(endpoint: str, block_id: str):
    channel = secure_channel(endpoint)
    stub = datanode_pb2_grpc.DataNodeStub(channel)

    return stub.StatBlock(
        datanode_pb2.BlockRequest(block_id=block_id),
        timeout=5,
    )


# Construye el plan de subida: bloques, destinos y capabilities temporales.
@router.post("/v1/uploads/start")
def start_upload(data: UploadStartRequest, user=Depends(current_user)):
    path = fs.normalize(data.path)

    if path == "/":
        raise HTTPException(
            status_code=409,
            detail="No se puede reemplazar la raíz",
        )

    parent_path = fs.parent(path)
    parent = fs.get_entry(user["user_id"], parent_path)

    if not parent or parent["type"] != "dir":
        raise HTTPException(
            status_code=404,
            detail="El directorio padre no existe",
        )

    existing = fs.get_entry(user["user_id"], path)

    if existing and existing["type"] == "dir":
        raise HTTPException(
            status_code=409,
            detail="La ruta corresponde a un directorio",
        )

    nodes = active_nodes()

    if len(nodes) < REPLICATION_FACTOR:
        raise HTTPException(
            status_code=503,
            detail="No hay suficientes DataNodes activos para R=2",
        )

    if existing:
        file_id = existing["file_id"]
    else:
        file_id = str(uuid.uuid4())

    current_key = f"/dfsha/v1/files/{file_id}/current"
    current_raw = etcd.get(current_key)
    base_version = int(current_raw) if current_raw else 0
    version = base_version + 1

    block_count = math.ceil(data.size / BLOCK_SIZE) if data.size else 0
    blocks = []

    for index in range(block_count):
        offset = index * BLOCK_SIZE
        size = min(BLOCK_SIZE, data.size - offset)

        replicas = [
            nodes[(index + replica_index) % len(nodes)]
            for replica_index in range(REPLICATION_FACTOR)
        ]

        blocks.append({
            "index": index,
            "block_id": (
                f"{file_id}-v{version}-b{index}-"
                f"{uuid.uuid4().hex[:8]}"
            ),
            "offset": offset,
            "size": size,
            "replicas": replicas,
        })

    upload_id = str(uuid.uuid4())

    operation = {
        "operation_id": upload_id,
        "type": "put",
        "status": "pending",
        "owner": user["user_id"],
        "path": path,
        "file_id": file_id,
        "base_version": base_version,
        "version": version,
        "size": data.size,
        "sha256": data.sha256,
        "block_size": BLOCK_SIZE,
        "replication_factor": REPLICATION_FACTOR,
        "blocks": blocks,
    }

    etcd.put(
        f"/dfsha/v1/operations/{upload_id}",
        json.dumps(operation),
    )

    # Las capacidades son efímeras: no se almacenan en etcd.
    # Se añaden únicamente a la respuesta enviada al cliente.
    client_operation = json.loads(json.dumps(operation))

    for block in client_operation["blocks"]:
        for replica in block["replicas"]:
            replica["capability"] = create_block_capability(
                block_id=block["block_id"],
                operation="put",
                node_id=replica["node_id"],
            )

    return client_operation


# Verifica bloques/réplicas e integridad antes de publicar la nueva versión del archivo.
@router.post("/v1/uploads/{upload_id}/commit")
def commit_upload(
    upload_id: str,
    data: UploadCommitRequest,
    user=Depends(current_user),
):
    op_key = f"/dfsha/v1/operations/{upload_id}"
    raw = etcd.get(op_key)

    if not raw:
        raise HTTPException(
            status_code=404,
            detail="Operación de carga no encontrada",
        )

    operation = json.loads(raw)

    if operation["owner"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Acceso denegado")

    if operation["status"] != "pending":
        raise HTTPException(
            status_code=409,
            detail="La operación ya fue finalizada",
        )

    current_key = f"/dfsha/v1/files/{operation['file_id']}/current"
    current_raw = etcd.get(current_key)
    current_version = int(current_raw) if current_raw else 0

    if current_version != operation["base_version"]:
        raise HTTPException(
            status_code=409,
            detail="La versión del archivo cambió durante la carga",
        )

    commits = {item.index: item for item in data.blocks}

    if len(commits) != len(operation["blocks"]):
        raise HTTPException(
            status_code=400,
            detail="Cantidad de bloques incompleta",
        )

    manifest_blocks = []

    for planned in operation["blocks"]:
        index = planned["index"]

        if index not in commits:
            raise HTTPException(
                status_code=400,
                detail=f"Falta confirmación del bloque {index}",
            )

        received = commits[index]

        if received.size != planned["size"]:
            raise HTTPException(
                status_code=409,
                detail=f"Tamaño inválido en bloque {index}",
            )

        confirmed_replicas = []

        for replica in planned["replicas"]:
            try:
                stat = stat_remote_block(
                    replica["endpoint"],
                    planned["block_id"],
                )
            except grpc.RpcError as exc:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"No se pudo confirmar {planned['block_id']} "
                        f"en {replica['node_id']}: {exc.code().name}"
                    ),
                )

            if (
                not stat.success
                or stat.size != received.size
                or stat.sha256 != received.sha256
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Réplica inconsistente de bloque {index} "
                        f"en {replica['node_id']}"
                    ),
                )

            confirmed_replicas.append(replica)

        if len(confirmed_replicas) < REPLICATION_FACTOR:
            raise HTTPException(
                status_code=503,
                detail=f"Bloque {index} no alcanzó R=2",
            )

        etcd.put(
            f"/dfsha/v1/blocks/{planned['block_id']}/replicas",
            json.dumps(confirmed_replicas),
        )

        manifest_blocks.append({
            "index": index,
            "block_id": planned["block_id"],
            "offset": planned["offset"],
            "size": received.size,
            "sha256": received.sha256,
        })

    manifest = {
        "file_id": operation["file_id"],
        "version": operation["version"],
        "owner": operation["owner"],
        "path": operation["path"],
        "size": operation["size"],
        "sha256": operation["sha256"],
        "block_size": operation["block_size"],
        "replication_factor": operation["replication_factor"],
        "blocks": manifest_blocks,
    }

    manifest_key = (
        f"/dfsha/v1/files/{operation['file_id']}/"
        f"versions/{operation['version']}"
    )

    etcd.put(manifest_key, json.dumps(manifest))

    # El puntero current se publica al final: hasta aquí la versión anterior
    # sigue siendo la visible.
    etcd.put(current_key, str(operation["version"]))

    file_entry = {
        "path": operation["path"],
        "type": "file",
        "owner": operation["owner"],
        "file_id": operation["file_id"],
        "current_version": operation["version"],
        "size": operation["size"],
        "sha256": operation["sha256"],
    }

    etcd.put(
        fs.key(operation["owner"], operation["path"]),
        json.dumps(file_entry),
    )

    operation["status"] = "committed"
    etcd.put(op_key, json.dumps(operation))

    return {
        "status": "committed",
        "path": operation["path"],
        "file_id": operation["file_id"],
        "version": operation["version"],
        "blocks": len(manifest_blocks),
        "replication_factor": REPLICATION_FACTOR,
    }


# Solicita el manifest vigente del archivo al plano de control.
@router.get("/v1/files/manifest")
def get_manifest(path: str, user=Depends(current_user)):
    path = fs.normalize(path)
    entry = fs.get_entry(user["user_id"], path)

    if not entry or entry["type"] != "file":
        raise HTTPException(
            status_code=404,
            detail="Archivo no encontrado",
        )

    file_id = entry["file_id"]
    current = etcd.get(f"/dfsha/v1/files/{file_id}/current")

    if not current:
        raise HTTPException(
            status_code=409,
            detail="Archivo sin versión publicada",
        )

    raw = etcd.get(
        f"/dfsha/v1/files/{file_id}/versions/{current}"
    )

    if not raw:
        raise HTTPException(
            status_code=409,
            detail="Manifiesto no disponible",
        )

    manifest = json.loads(raw)

    for block in manifest["blocks"]:
        replicas_raw = etcd.get(
            f"/dfsha/v1/blocks/{block['block_id']}/replicas"
        )

        replicas = (
            json.loads(replicas_raw)
            if replicas_raw
            else []
        )

        block["replicas"] = [
            {
                **replica,
                "capability": create_block_capability(
                    block_id=block["block_id"],
                    operation="get",
                    node_id=replica["node_id"],
                ),
            }
            for replica in replicas
        ]

    return manifest
