# -----------------------------------------------------------------------------
# Prueba integrada de D1, D2 y D3 usando mTLS, capability temporal y verificación SHA-256.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import hashlib
import sys
import uuid
from pathlib import Path

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "datanode"))

from shared.grpc_tls import secure_channel
from shared.capability import create_block_capability
import datanode_pb2
import datanode_pb2_grpc


NODES = [
    ("d1", "172.31.21.156:50051"),
    ("d2", "172.31.25.229:50051"),
    ("d3", "172.31.28.137:50051"),
]


# Genera la capability temporal usada por esta prueba integrada.
def capability(block_id, operation, node_id):
    try:
        return create_block_capability(
            block_id=block_id,
            operation=operation,
            node_id=node_id,
        )
    except TypeError:
        return create_block_capability(
            block_id=block_id,
            op=operation,
            node_id=node_id,
        )


print("=== DFSha - PRUEBA FINAL DE 3 DATANODES ===")
print("Seguridad: mTLS + capabilities temporales")
print()

ok = 0

for node_id, endpoint in NODES:
    block_id = f"final-{node_id}-{uuid.uuid4().hex[:8]}"
    data = f"DFSha prueba final autorizada {node_id}".encode()
    expected_sha = hashlib.sha256(data).hexdigest()

    channel = secure_channel(endpoint)
    stub = datanode_pb2_grpc.DataNodeStub(channel)

    try:
        put_token = capability(block_id, "put", node_id)

        ack = stub.PutBlock(
            iter([
                datanode_pb2.BlockChunk(
                    block_id=block_id,
                    data=data,
                )
            ]),
            metadata=(("x-dfsha-capability", put_token),),
            timeout=5,
        )

        get_token = capability(block_id, "get", node_id)

        recovered = b"".join(
            chunk.data
            for chunk in stub.GetBlock(
                datanode_pb2.BlockRequest(block_id=block_id),
                metadata=(("x-dfsha-capability", get_token),),
                timeout=5,
            )
        )

        actual_sha = hashlib.sha256(recovered).hexdigest()

        if ack.success and actual_sha == expected_sha:
            print(
                f"[OK] {node_id.upper()} {endpoint} | "
                f"PUT+GET autorizado | SHA-256=OK"
            )
            ok += 1
        else:
            print(f"[ERROR] {node_id.upper()} | integridad incorrecta")

    except Exception as exc:
        print(f"[ERROR] {node_id.upper()} {endpoint} | {exc}")

    finally:
        channel.close()

print()
print(f"RESULTADO: {ok}/3 DATANODES OPERATIVOS")
print(
    "INFRAESTRUCTURA DE DATOS: "
    + ("OK" if ok == 3 else "ERROR")
)
