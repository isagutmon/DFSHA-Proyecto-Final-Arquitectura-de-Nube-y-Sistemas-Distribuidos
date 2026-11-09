# -----------------------------------------------------------------------------
# Prueba negativa: un cliente con mTLS válido no puede leer sin la capability temporal requerida.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import sys
from pathlib import Path

import grpc

PROJECT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT))
sys.path.insert(0, str(PROJECT / "datanode"))

from shared.grpc_tls import secure_channel
import datanode_pb2
import datanode_pb2_grpc

DATA_DIR = Path("/mnt/dfsha-data/d1/blocks")
ENDPOINT = "172.31.21.156:50051"

blocks = [
    p for p in DATA_DIR.iterdir()
    if p.is_file() and not p.name.endswith(".part")
]

if not blocks:
    raise SystemExit("No hay bloques disponibles para probar")

block_id = blocks[0].name

print("=== PRUEBA DE CAPABILITY ===")
print(f"DataNode: D1 ({ENDPOINT})")
print(f"Bloque: {block_id}")
print("mTLS: certificado de cliente válido")
print("Capability GET: NO enviada")

channel = secure_channel(ENDPOINT)
stub = datanode_pb2_grpc.DataNodeStub(channel)

try:
    list(
        stub.GetBlock(
            datanode_pb2.BlockRequest(block_id=block_id),
            timeout=5,
        )
    )

    print("RESULTADO: ERROR - acceso permitido sin capability")

except grpc.RpcError as exc:
    print(f"Respuesta gRPC: {exc.code().name}")
    print(f"Detalle: {exc.details()}")

    if exc.code() == grpc.StatusCode.PERMISSION_DENIED:
        print("RESULTADO: ACCESO RECHAZADO")
        print("CAPABILITY OK: el DataNode exige permiso temporal")
    else:
        print("RESULTADO: rechazo por causa inesperada")

finally:
    channel.close()
