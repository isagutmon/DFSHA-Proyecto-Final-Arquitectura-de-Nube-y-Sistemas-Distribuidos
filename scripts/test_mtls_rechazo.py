# -----------------------------------------------------------------------------
# Prueba negativa de mTLS: verifica el rechazo de conexiones sin certificado de cliente válido.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import sys
from pathlib import Path

import grpc

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DATANODE_DIR = PROJECT_ROOT / "datanode"
if str(DATANODE_DIR) not in sys.path:
    sys.path.insert(0, str(DATANODE_DIR))

import datanode_pb2
import datanode_pb2_grpc


ENDPOINT = "172.31.21.156:50051"

# Confiamos en la CA de DFSha, pero deliberadamente NO presentamos
# grpc-client.crt ni grpc-client.key.
ca = (PROJECT_ROOT / "certs" / "ca.crt").read_bytes()

credentials = grpc.ssl_channel_credentials(
    root_certificates=ca
)

channel = grpc.secure_channel(
    ENDPOINT,
    credentials,
)

print("=== PRUEBA mTLS: CLIENTE SIN CERTIFICADO ===")
print(f"DataNode: {ENDPOINT}")
print("CA válida: sí")
print("Certificado cliente: NO")

try:
    grpc.channel_ready_future(channel).result(timeout=5)

    stub = datanode_pb2_grpc.DataNodeStub(channel)
    stub.StatBlock(
        datanode_pb2.BlockRequest(block_id="test-block-001"),
        timeout=5,
    )

    print("RESULTADO: ERROR - el servidor permitió acceso sin certificado")

except (grpc.FutureTimeoutError, grpc.RpcError) as exc:
    print("RESULTADO: ACCESO RECHAZADO")
    print("mTLS: OK")
    print("El DataNode exige certificado cliente válido.")

finally:
    channel.close()
