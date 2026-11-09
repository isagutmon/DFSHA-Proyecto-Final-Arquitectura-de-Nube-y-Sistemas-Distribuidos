# -----------------------------------------------------------------------------
# Prueba remota del DataNode D2 para validar transferencia e integridad por red.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import os
import sys
import hashlib
import grpc

sys.path.insert(0, os.path.expanduser("~/dfsha/datanode"))

import datanode_pb2
import datanode_pb2_grpc
from shared.grpc_tls import secure_channel

SERVER = "172.31.25.229:50051"
BLOCK_ID = "remote-test-d2-001"
ORIGINAL = "/tmp/d2-original.bin"
RECOVERED = "/tmp/d2-recovered.bin"
CHUNK_SIZE = 256 * 1024

# Calcula SHA-256 del archivo local para comprobar integridad extremo a extremo.
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            h.update(data)
    return h.hexdigest()

# Genera los fragmentos de datos utilizados por la prueba de transferencia.
def chunks():
    with open(ORIGINAL, "rb") as f:
        while True:
            data = f.read(CHUNK_SIZE)
            if not data:
                break
            yield datanode_pb2.BlockChunk(
                block_id=BLOCK_ID,
                data=data
            )

with open(ORIGINAL, "wb") as f:
    f.write(os.urandom(1024 * 1024))

channel = secure_channel(SERVER)
stub = datanode_pb2_grpc.DataNodeStub(channel)

print("Servidor:", SERVER)

print("\n1. PUT remoto")
ack = stub.PutBlock(chunks())
print(ack)

print("\n2. STAT remoto")
stat = stub.StatBlock(
    datanode_pb2.BlockRequest(block_id=BLOCK_ID)
)
print(stat)

print("\n3. GET remoto")
with open(RECOVERED, "wb") as f:
    for chunk in stub.GetBlock(
        datanode_pb2.BlockRequest(block_id=BLOCK_ID)
    ):
        f.write(chunk.data)

original_hash = sha256_file(ORIGINAL)
recovered_hash = sha256_file(RECOVERED)

print("\nSHA original   :", original_hash)
print("SHA D2         :", stat.sha256)
print("SHA recuperado :", recovered_hash)
print(
    "INTEGRIDAD REMOTA OK:",
    original_hash == recovered_hash == stat.sha256
)
