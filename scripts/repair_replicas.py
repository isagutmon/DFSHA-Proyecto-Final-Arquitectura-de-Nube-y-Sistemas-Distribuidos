# -----------------------------------------------------------------------------
# Reparación explícita de réplicas: detecta pérdidas, copia desde una réplica sana y actualiza metadatos.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import argparse
import hashlib
import json
import os
import sys
import time

import grpc

PROJECT = os.path.expanduser("~/dfsha")
sys.path.insert(0, PROJECT)
sys.path.insert(0, os.path.join(PROJECT, "datanode"))

from metadata.etcd_store import EtcdStore
from shared.capability import create_block_capability
import datanode_pb2
import datanode_pb2_grpc
from shared.grpc_tls import secure_channel


R = 2
RPC_CHUNK = 256 * 1024


# Comprueba si un DataNode candidato está accesible antes de una reparación.
def reachable(endpoint: str, timeout: float = 1.5) -> bool:
    channel = secure_channel(endpoint)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout)
        return True
    except grpc.FutureTimeoutError:
        return False
    finally:
        channel.close()


# Consulta el bloque remoto para verificar tamaño/hash antes de usarlo.
def stat_block(endpoint: str, block_id: str):
    channel = secure_channel(endpoint)
    try:
        stub = datanode_pb2_grpc.DataNodeStub(channel)
        return stub.StatBlock(
            datanode_pb2.BlockRequest(block_id=block_id),
            timeout=5,
        )
    finally:
        channel.close()


# Descarga desde una réplica fuente usando una capability GET temporal.
def get_block(
    endpoint: str,
    block_id: str,
    node_id: str,
) -> bytes:
    channel = secure_channel(endpoint)
    try:
        stub = datanode_pb2_grpc.DataNodeStub(channel)
        data = bytearray()

        capability = create_block_capability(
            block_id=block_id,
            operation="get",
            node_id=node_id,
        )

        for chunk in stub.GetBlock(
            datanode_pb2.BlockRequest(block_id=block_id),
            timeout=20,
            metadata=(
                ("x-dfsha-capability", capability),
            ),
        ):
            data.extend(chunk.data)

        return bytes(data)
    finally:
        channel.close()


# Envía un bloque a un DataNode usando el canal seguro y la capability correspondiente.
def put_block(
    endpoint: str,
    block_id: str,
    data: bytes,
    node_id: str,
):
    channel = secure_channel(endpoint)
    try:
        stub = datanode_pb2_grpc.DataNodeStub(channel)

        capability = create_block_capability(
            block_id=block_id,
            operation="put",
            node_id=node_id,
        )

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
                ("x-dfsha-capability", capability),
            ),
        )
    finally:
        channel.close()


# Obtiene los bloques de las versiones actuales que deben conservar el factor de replicación.
def current_blocks(store: EtcdStore, path_filter=None):
    seen_files = set()
    blocks = []

    for _, raw in store.get_prefix("/dfsha/v1/entries/"):
        try:
            entry = json.loads(raw)
        except Exception:
            continue

        if entry.get("type") != "file":
            continue

        if path_filter and entry.get("path") != path_filter:
            continue

        file_id = entry.get("file_id")
        owner = entry.get("owner")

        if not file_id or file_id in seen_files:
            continue

        seen_files.add(file_id)

        current = store.get(f"/dfsha/v1/files/{file_id}/current")
        if not current:
            continue

        manifest_raw = store.get(
            f"/dfsha/v1/files/{file_id}/versions/{current}"
        )
        if not manifest_raw:
            continue

        manifest = json.loads(manifest_raw)

        for block in manifest.get("blocks", []):
            blocks.append({
                "file_id": file_id,
                "owner": owner,
                "path": entry.get("path"),
                "version": int(current),
                "index": block["index"],
                "block_id": block["block_id"],
                "size": block["size"],
                "sha256": block["sha256"],
            })

    return blocks


# Obtiene los DataNodes registrados y sus endpoints.
def registered_nodes(store: EtcdStore):
    nodes = []

    for _, raw in store.get_prefix("/dfsha/v1/nodes/"):
        try:
            node = json.loads(raw)
        except Exception:
            continue

        if node.get("status") == "active":
            nodes.append(node)

    return sorted(nodes, key=lambda n: n["node_id"])


# Ejecuta una pasada de reparación para restaurar R=2 donde falten réplicas.
def repair_once(path_filter=None):
    store = EtcdStore()
    nodes = registered_nodes(store)
    blocks = current_blocks(store, path_filter)

    print("=== DFSha - REPARACION DE REPLICAS ===")
    if path_filter:
        print("FILTRO DE ARCHIVO:", path_filter)

    print("DATANODES REGISTRADOS:", ",".join(n["node_id"] for n in nodes))
    print("BLOQUES ACTUALES A REVISAR:", len(blocks))
    print()

    repaired = 0
    healthy_total = 0
    unrecoverable = 0
    skipped = 0

    for block in blocks:
        replica_key = f"/dfsha/v1/blocks/{block['block_id']}/replicas"
        replicas_raw = store.get(replica_key)

        if not replicas_raw:
            print(
                f"[ERROR] {block['path']} b{block['index']}: "
                "sin metadatos de replicas"
            )
            unrecoverable += 1
            continue

        replicas = json.loads(replicas_raw)

        healthy = []
        failed = []

        for replica in replicas:
            endpoint = replica["endpoint"]

            if not reachable(endpoint):
                failed.append(replica)
                continue

            try:
                stat = stat_block(endpoint, block["block_id"])
            except grpc.RpcError:
                failed.append(replica)
                continue

            if (
                stat.success
                and stat.size == block["size"]
                and stat.sha256 == block["sha256"]
            ):
                healthy.append(replica)
            else:
                failed.append(replica)

        before_ids = ",".join(r["node_id"] for r in replicas)

        if len(healthy) >= R:
            healthy_total += 1
            print(
                f"[OK] {block['path']} b{block['index']} "
                f"| replicas={before_ids} | R={len(healthy)}"
            )
            continue

        if not healthy:
            print(
                f"[NO REPARABLE] {block['path']} b{block['index']} "
                "| no queda replica sana"
            )
            unrecoverable += 1
            continue

        source = healthy[0]

        candidates = [
            node for node in nodes
            if node["node_id"] not in {r["node_id"] for r in healthy}
            and reachable(node["endpoint"])
        ]

        if not candidates:
            print(
                f"[PENDIENTE] {block['path']} b{block['index']} "
                "| no hay nodo alternativo sano"
            )
            skipped += 1
            continue

        target = candidates[0]

        data = get_block(
            source["endpoint"],
            block["block_id"],
            source["node_id"],
        )
        digest = hashlib.sha256(data).hexdigest()

        if len(data) != block["size"] or digest != block["sha256"]:
            print(
                f"[ERROR] {block['path']} b{block['index']} "
                "| fuente no coincide con manifiesto"
            )
            unrecoverable += 1
            continue

        # Si el target ya tiene una copia íntegra, la reutilizamos.
        target_ok = False
        try:
            stat = stat_block(target["endpoint"], block["block_id"])
            target_ok = (
                stat.success
                and stat.size == block["size"]
                and stat.sha256 == block["sha256"]
            )
        except grpc.RpcError:
            target_ok = False

        if not target_ok:
            ack = put_block(
                target["endpoint"],
                block["block_id"],
                data,
                target["node_id"],
            )

            if (
                not ack.success
                or ack.size != block["size"]
                or ack.sha256 != block["sha256"]
            ):
                print(
                    f"[ERROR] {block['path']} b{block['index']} "
                    f"| fallo al copiar hacia {target['node_id']}"
                )
                unrecoverable += 1
                continue

        # Replica list nueva: copia sana + nuevo destino.
        new_replicas = [source, target]
        new_raw = json.dumps(new_replicas)

        txn = store.txn_compare_value_put(
            replica_key,
            replicas_raw,
            new_raw,
        )

        if not txn.get("succeeded"):
            print(
                f"[REINTENTAR] {block['path']} b{block['index']} "
                "| ubicaciones cambiaron concurrentemente"
            )
            skipped += 1
            continue

        verify = stat_block(target["endpoint"], block["block_id"])
        verified = (
            verify.success
            and verify.size == block["size"]
            and verify.sha256 == block["sha256"]
        )

        failed_ids = ",".join(r["node_id"] for r in failed) or "-"
        after_ids = ",".join(r["node_id"] for r in new_replicas)

        print(
            f"[REPARADO] {block['path']} b{block['index']} "
            f"| antes={before_ids} "
            f"| perdida={failed_ids} "
            f"| fuente={source['node_id']} "
            f"| destino={target['node_id']} "
            f"| despues={after_ids}"
        )
        print(
            f"           SHA-256 VERIFICADO EN {target['node_id']}: "
            f"{str(verified).upper()}"
        )

        repaired += 1

    print()
    print("=== RESUMEN ===")
    print("BLOQUES YA SANOS:", healthy_total)
    print("BLOQUES REPARADOS:", repaired)
    print("BLOQUES PENDIENTES:", skipped)
    print("BLOQUES NO REPARABLES:", unrecoverable)

    ok = unrecoverable == 0 and skipped == 0
    print("REPARACION FINALIZADA:", str(ok).upper())

    return 0 if ok else 2


# Punto de entrada del script.
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--path",
        help="Reparar solo la version actual de esta ruta logica",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=0,
        help="Si es >0, repetir continuamente cada N segundos",
    )
    args = parser.parse_args()

    if args.interval <= 0:
        raise SystemExit(repair_once(args.path))

    while True:
        repair_once(args.path)
        print(f"\nEsperando {args.interval}s para la siguiente revision...\n")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
