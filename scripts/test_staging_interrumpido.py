# -----------------------------------------------------------------------------
# Prueba de staging seguro: interrumpe una escritura para comprobar .part y limpieza posterior.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import sys
import time
from pathlib import Path

import grpc

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "datanode"))

from shared.grpc_tls import secure_channel
from shared.capability import create_block_capability
import datanode_pb2
import datanode_pb2_grpc

BLOCK_ID = "staging-fallo-prueba"
ENDPOINT = "172.31.21.156:50051"
NODE_ID = "d1"

try:
    token = create_block_capability(
        block_id=BLOCK_ID,
        operation="put",
        node_id=NODE_ID,
    )
except TypeError:
    token = create_block_capability(
        block_id=BLOCK_ID,
        op="put",
        node_id=NODE_ID,
    )

# Genera el stream de mensajes gRPC utilizado en una transferencia de bloque.
def stream():
    chunk = b"S" * (256 * 1024)

    for i in range(128):
        yield datanode_pb2.BlockChunk(
            block_id=BLOCK_ID,
            data=chunk,
        )
        print(f"Chunk enviado: {i + 1}/128", flush=True)
        time.sleep(0.2)

channel = secure_channel(ENDPOINT)
stub = datanode_pb2_grpc.DataNodeStub(channel)

print("=== PRUEBA STAGING INTERRUMPIDO ===")
print("Enviando bloque lentamente a D1...")
print("Ahora D1 debe ser detenido mientras existe el .part")

try:
    response = stub.PutBlock(
        stream(),
        metadata=(("x-dfsha-capability", token),),
        timeout=60,
    )
    print("Respuesta:", response)

except grpc.RpcError as exc:
    print(f"TRANSFERENCIA INTERRUMPIDA: {exc.code().name}")
    print("Esto es esperado al detener D1.")

finally:
    channel.close()
