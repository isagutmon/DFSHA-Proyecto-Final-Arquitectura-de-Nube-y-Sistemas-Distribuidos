# -----------------------------------------------------------------------------
# Endpoints RF3: handles, locks con lease, planes de lectura/escritura, COW y commit por versión.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import hashlib
import json
import os
import sys
import uuid

import grpc
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from metadata.etcd_store import EtcdStore
from metadata.fs_store import FSStore
from shared.config import REPLICATION_FACTOR
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

LOCK_TTL_SECONDS = 60


# Modelo de solicitud para abrir un archivo RF3.
class OpenRequest(BaseModel):
    path: str
    mode: str = "r"


# Modelo que transporta el identificador del handle RF3.
class HandleRequest(BaseModel):
    handle_id: str


# Modelo para solicitar un plan de lectura por rango.
class ReadPlanRequest(BaseModel):
    handle_id: str
    offset: int = Field(ge=0)
    length: int = Field(gt=0)


# Modelo para solicitar un plan de escritura por rango.
class WritePlanRequest(BaseModel):
    handle_id: str
    offset: int = Field(ge=0)
    size: int = Field(gt=0)
    patch_sha256: str = Field(min_length=64, max_length=64)


# Describe un bloque nuevo producido por una escritura Copy-on-Write.
class ChangedBlock(BaseModel):
    index: int
    size: int = Field(gt=0)
    sha256: str = Field(min_length=64, max_length=64)


# Modelo usado para confirmar los nuevos bloques y publicar la versión RF3.
class WriteCommitRequest(BaseModel):
    handle_id: str
    blocks: list[ChangedBlock]
    file_sha256: str = Field(min_length=64, max_length=64)


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


# Construye la clave etcd donde se almacena temporalmente un handle.
def handle_key(handle_id):
    return f"/dfsha/v1/handles/{handle_id}"


# Construye la clave etcd usada como lock exclusivo por archivo.
def lock_key(file_id):
    return f"/dfsha/v1/locks/{file_id}"


# Carga y valida el handle, incluyendo su pertenencia al usuario autenticado.
def load_handle(handle_id, user_id):
    raw = etcd.get(handle_key(handle_id))

    if not raw:
        raise HTTPException(
            status_code=404,
            detail="Handle no encontrado o cerrado",
        )

    handle = json.loads(raw)

    if handle["owner"] != user_id:
        raise HTTPException(status_code=403, detail="Acceso denegado")

    return handle


# Carga el manifest inmutable correspondiente a una versión concreta.
def load_manifest(file_id, version):
    raw = etcd.get(
        f"/dfsha/v1/files/{file_id}/versions/{version}"
    )

    if not raw:
        raise HTTPException(
            status_code=409,
            detail="Versión del archivo no disponible",
        )

    manifest = json.loads(raw)

    for block in manifest["blocks"]:
        replicas_raw = etcd.get(
            f"/dfsha/v1/blocks/{block['block_id']}/replicas"
        )
        block["replicas"] = (
            json.loads(replicas_raw)
            if replicas_raw
            else []
        )

    return manifest


# Verifica que el handle posee el lock vigente antes de permitir una escritura.
def require_lock(handle):
    raw = etcd.get(lock_key(handle["file_id"]))

    if not raw:
        raise HTTPException(
            status_code=423,
            detail="El archivo no está bloqueado por este handle",
        )

    lock = json.loads(raw)

    if (
        lock.get("handle_id") != handle["handle_id"]
        or lock.get("owner") != handle["owner"]
    ):
        raise HTTPException(
            status_code=423,
            detail="Lock pertenece a otro cliente",
        )

    return raw, lock


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


# Consulta metadatos de un bloque remoto para validar su existencia e integridad.
def stat_remote(endpoint, block_id):
    channel = secure_channel(endpoint)
    stub = datanode_pb2_grpc.DataNodeStub(channel)

    return stub.StatBlock(
        datanode_pb2.BlockRequest(block_id=block_id),
        timeout=5,
    )


# Abre un archivo sobre la versión actual y crea un handle/snapshot temporal.
@router.post("/v1/files/open")
def open_file(data: OpenRequest, user=Depends(current_user)):
    mode = data.mode.lower()

    if mode not in {"r", "rw"}:
        raise HTTPException(
            status_code=400,
            detail="Modo válido: r o rw",
        )

    path = fs.normalize(data.path)
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

    handle_id = str(uuid.uuid4())

    handle = {
        "handle_id": handle_id,
        "owner": user["user_id"],
        "path": path,
        "file_id": file_id,
        "version": int(current),
        "mode": mode,
    }

    etcd.put(handle_key(handle_id), json.dumps(handle))

    return handle


# Cierra el handle y revoca los recursos temporales asociados, incluido el lock propio.
@router.post("/v1/files/close")
def close_file(data: HandleRequest, user=Depends(current_user)):
    handle = load_handle(data.handle_id, user["user_id"])
    lk = lock_key(handle["file_id"])
    raw = etcd.get(lk)

    if raw:
        lock = json.loads(raw)

        if lock.get("handle_id") == handle["handle_id"]:
            result = etcd.txn_compare_value_delete(lk, raw)

            if result.get("succeeded"):
                etcd.lease_revoke(lock.get("lease_id"))

    etcd.delete(handle_key(data.handle_id))

    return {
        "closed": data.handle_id,
        "path": handle["path"],
    }


# Adquiere mediante etcd un lock exclusivo con lease para un escritor.
@router.post("/v1/files/lock")
def acquire_lock(data: HandleRequest, user=Depends(current_user)):
    handle = load_handle(data.handle_id, user["user_id"])

    if handle["mode"] != "rw":
        raise HTTPException(
            status_code=409,
            detail="El handle no fue abierto en modo rw",
        )

    lease_id = etcd.lease_grant(LOCK_TTL_SECONDS)

    lock = {
        "file_id": handle["file_id"],
        "handle_id": handle["handle_id"],
        "owner": handle["owner"],
        "lease_id": str(lease_id),
        "ttl": LOCK_TTL_SECONDS,
    }

    raw = json.dumps(lock, sort_keys=True)

    result = etcd.txn_create_with_lease(
        lock_key(handle["file_id"]),
        raw,
        lease_id,
    )

    if not result.get("succeeded"):
        etcd.lease_revoke(lease_id)
        raise HTTPException(
            status_code=423,
            detail="Archivo bloqueado por otro cliente",
        )

    return {
        "locked": True,
        "handle_id": handle["handle_id"],
        "ttl": LOCK_TTL_SECONDS,
    }


# Renueva el lease del lock exclusivo cuando el escritor continúa activo.
@router.post("/v1/files/lock/renew")
def renew_lock(data: HandleRequest, user=Depends(current_user)):
    handle = load_handle(data.handle_id, user["user_id"])
    old_raw, old_lock = require_lock(handle)

    new_lease = etcd.lease_grant(LOCK_TTL_SECONDS)

    new_lock = dict(old_lock)
    new_lock["lease_id"] = str(new_lease)
    new_lock["ttl"] = LOCK_TTL_SECONDS
    new_raw = json.dumps(new_lock, sort_keys=True)

    result = etcd.txn_compare_value_put(
        lock_key(handle["file_id"]),
        old_raw,
        new_raw,
        new_lease,
    )

    if not result.get("succeeded"):
        etcd.lease_revoke(new_lease)
        raise HTTPException(
            status_code=423,
            detail="No se pudo renovar el lock",
        )

    etcd.lease_revoke(old_lock.get("lease_id"))

    return {
        "renewed": True,
        "handle_id": handle["handle_id"],
        "ttl": LOCK_TTL_SECONDS,
    }


# Calcula qué bloques y rangos debe leer el cliente y emite capabilities GET.
@router.post("/v1/files/read-plan")
def read_plan(data: ReadPlanRequest, user=Depends(current_user)):
    handle = load_handle(data.handle_id, user["user_id"])
    manifest = load_manifest(handle["file_id"], handle["version"])

    if data.offset >= manifest["size"]:
        raise HTTPException(
            status_code=416,
            detail="Offset fuera del archivo",
        )

    end = min(data.offset + data.length, manifest["size"])
    segments = []

    for block in manifest["blocks"]:
        block_start = block["offset"]
        block_end = block_start + block["size"]

        start = max(data.offset, block_start)
        stop = min(end, block_end)

        if start >= stop:
            continue

        segments.append({
            "index": block["index"],
            "block_id": block["block_id"],
            "block_offset": start - block_start,
            "length": stop - start,
            "output_offset": start - data.offset,
            "sha256": block["sha256"],
            "size": block["size"],
            "replicas": [
                {
                    **replica,
                    "capability": create_block_capability(
                        block_id=block["block_id"],
                        operation="get",
                        node_id=replica["node_id"],
                    ),
                }
                for replica in block["replicas"]
            ],
        })

    return {
        "handle_id": handle["handle_id"],
        "version": handle["version"],
        "requested_offset": data.offset,
        "requested_length": data.length,
        "actual_length": end - data.offset,
        "segments": segments,
    }


# Calcula los bloques afectados por la escritura y prepara destinos/capabilities para COW.
@router.post("/v1/files/write-plan")
def write_plan(data: WritePlanRequest, user=Depends(current_user)):
    handle = load_handle(data.handle_id, user["user_id"])

    if handle["mode"] != "rw":
        raise HTTPException(
            status_code=409,
            detail="Handle no abierto en modo rw",
        )

    require_lock(handle)

    manifest = load_manifest(handle["file_id"], handle["version"])

    if data.offset + data.size > manifest["size"]:
        raise HTTPException(
            status_code=416,
            detail=(
                "write no puede extender el archivo en la "
                "semántica RF3 adoptada"
            ),
        )

    current = etcd.get(
        f"/dfsha/v1/files/{handle['file_id']}/current"
    )

    if current != str(handle["version"]):
        raise HTTPException(
            status_code=409,
            detail="VERSION_CONFLICT",
        )

    nodes = active_nodes()

    if len(nodes) < REPLICATION_FACTOR:
        raise HTTPException(
            status_code=503,
            detail="No hay suficientes DataNodes para R=2",
        )

    new_version = handle["version"] + 1
    affected = []

    for block in manifest["blocks"]:
        block_start = block["offset"]
        block_end = block_start + block["size"]
        write_start = data.offset
        write_end = data.offset + data.size

        start = max(write_start, block_start)
        stop = min(write_end, block_end)

        if start >= stop:
            continue

        replicas = [
            nodes[(block["index"] + i) % len(nodes)]
            for i in range(REPLICATION_FACTOR)
        ]

        affected.append({
            "index": block["index"],
            "old_block_id": block["block_id"],
            "old_sha256": block["sha256"],
            "old_size": block["size"],
            "old_replicas": block["replicas"],
            "block_offset": start - block_start,
            "patch_offset": start - write_start,
            "patch_length": stop - start,
            "new_block_id": (
                f"{handle['file_id']}-v{new_version}-"
                f"b{block['index']}-{uuid.uuid4().hex[:8]}"
            ),
            "new_replicas": replicas,
        })

    operation_id = str(uuid.uuid4())

    operation = {
        "operation_id": operation_id,
        "type": "write",
        "status": "pending",
        "owner": user["user_id"],
        "handle_id": handle["handle_id"],
        "file_id": handle["file_id"],
        "path": handle["path"],
        "base_version": handle["version"],
        "new_version": new_version,
        "offset": data.offset,
        "size": data.size,
        "patch_sha256": data.patch_sha256,
        "affected": affected,
    }

    etcd.put(
        f"/dfsha/v1/operations/{operation_id}",
        json.dumps(operation),
    )

    # Las capacidades son temporales y no se persisten en etcd.
    # Para COW el cliente necesita:
    # - GET sobre el bloque anterior.
    # - PUT sobre el bloque nuevo.
    client_operation = json.loads(json.dumps(operation))

    for item in client_operation["affected"]:
        item["old_replicas"] = [
            {
                **replica,
                "capability": create_block_capability(
                    block_id=item["old_block_id"],
                    operation="get",
                    node_id=replica["node_id"],
                ),
            }
            for replica in item["old_replicas"]
        ]

        item["new_replicas"] = [
            {
                **replica,
                "capability": create_block_capability(
                    block_id=item["new_block_id"],
                    operation="put",
                    node_id=replica["node_id"],
                ),
            }
            for replica in item["new_replicas"]
        ]

    return client_operation


# Valida los bloques COW y publica la nueva versión mediante control de concurrencia/CAS.
@router.post("/v1/writes/{operation_id}/commit")
def write_commit(
    operation_id: str,
    data: WriteCommitRequest,
    user=Depends(current_user),
):
    raw = etcd.get(f"/dfsha/v1/operations/{operation_id}")

    if not raw:
        raise HTTPException(
            status_code=404,
            detail="Operación write no encontrada",
        )

    operation = json.loads(raw)

    if operation["owner"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Acceso denegado")

    if operation["handle_id"] != data.handle_id:
        raise HTTPException(status_code=409, detail="Handle incorrecto")

    handle = load_handle(data.handle_id, user["user_id"])
    require_lock(handle)

    if handle["version"] != operation["base_version"]:
        raise HTTPException(
            status_code=409,
            detail="VERSION_CONFLICT",
        )

    current_key = f"/dfsha/v1/files/{handle['file_id']}/current"
    current = etcd.get(current_key)

    if current != str(operation["base_version"]):
        raise HTTPException(
            status_code=409,
            detail="VERSION_CONFLICT",
        )

    confirmations = {item.index: item for item in data.blocks}
    base_manifest = load_manifest(
        handle["file_id"],
        operation["base_version"],
    )

    replacements = {}

    for planned in operation["affected"]:
        index = planned["index"]

        if index not in confirmations:
            raise HTTPException(
                status_code=400,
                detail=f"Falta confirmación del bloque {index}",
            )

        confirmation = confirmations[index]

        if confirmation.size != planned["old_size"]:
            raise HTTPException(
                status_code=409,
                detail=f"Tamaño inválido en bloque {index}",
            )

        confirmed_replicas = []

        for replica in planned["new_replicas"]:
            try:
                stat = stat_remote(
                    replica["endpoint"],
                    planned["new_block_id"],
                )
            except grpc.RpcError as exc:
                raise HTTPException(
                    status_code=503,
                    detail=(
                        f"No se confirmó bloque {index} en "
                        f"{replica['node_id']}: {exc.code().name}"
                    ),
                )

            if (
                not stat.success
                or stat.size != confirmation.size
                or stat.sha256 != confirmation.sha256
            ):
                raise HTTPException(
                    status_code=409,
                    detail=f"Réplica inconsistente del bloque {index}",
                )

            confirmed_replicas.append(replica)

        if len(confirmed_replicas) < REPLICATION_FACTOR:
            raise HTTPException(
                status_code=503,
                detail=f"Bloque {index} no alcanzó R=2",
            )

        etcd.put(
            f"/dfsha/v1/blocks/{planned['new_block_id']}/replicas",
            json.dumps(confirmed_replicas),
        )

        replacements[index] = {
            "index": index,
            "block_id": planned["new_block_id"],
            "offset": base_manifest["blocks"][index]["offset"],
            "size": confirmation.size,
            "sha256": confirmation.sha256,
        }

    new_blocks = []

    for block in base_manifest["blocks"]:
        if block["index"] in replacements:
            new_blocks.append(replacements[block["index"]])
        else:
            new_blocks.append({
                "index": block["index"],
                "block_id": block["block_id"],
                "offset": block["offset"],
                "size": block["size"],
                "sha256": block["sha256"],
            })

    new_manifest = {
        "file_id": handle["file_id"],
        "version": operation["new_version"],
        "owner": handle["owner"],
        "path": handle["path"],
        "size": base_manifest["size"],
        "sha256": data.file_sha256,
        "block_size": base_manifest["block_size"],
        "replication_factor": base_manifest["replication_factor"],
        "blocks": new_blocks,
    }

    file_entry = fs.get_entry(handle["owner"], handle["path"])
    file_entry["current_version"] = operation["new_version"]
    file_entry["sha256"] = data.file_sha256

    manifest_key = (
        f"/dfsha/v1/files/{handle['file_id']}/"
        f"versions/{operation['new_version']}"
    )

    txn = etcd.txn_compare_value_multi_put(
        current_key,
        str(operation["base_version"]),
        [
            (manifest_key, json.dumps(new_manifest)),
            (current_key, str(operation["new_version"])),
            (
                fs.key(handle["owner"], handle["path"]),
                json.dumps(file_entry),
            ),
        ],
    )

    if not txn.get("succeeded"):
        raise HTTPException(
            status_code=409,
            detail="VERSION_CONFLICT",
        )

    handle["version"] = operation["new_version"]
    etcd.put(handle_key(handle["handle_id"]), json.dumps(handle))

    operation["status"] = "committed"
    etcd.put(
        f"/dfsha/v1/operations/{operation_id}",
        json.dumps(operation),
    )

    return {
        "status": "committed",
        "path": handle["path"],
        "version": operation["new_version"],
        "changed_blocks": len(replacements),
        "sha256": data.file_sha256,
    }
