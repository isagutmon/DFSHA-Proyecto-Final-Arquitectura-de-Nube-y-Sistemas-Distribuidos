# -----------------------------------------------------------------------------
# Prueba directa del DataNode D1 para validar PUT/GET e integridad de bloques.
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


BLOCK_ID = "test-block-001"
TEST_FILE = "/tmp/dfsha-original.bin"
RECOVERED_FILE = "/tmp/dfsha-recovered.bin"
CHUNK_SIZE = 256 * 1024


# Calcula SHA-256 del archivo local para comprobar integridad extremo a extremo.
def sha256_file(path):
    h = hashlib.sha256()

    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)

    return h.hexdigest()


# Genera los fragmentos de datos utilizados por la prueba de transferencia.
def chunks():
    with open(TEST_FILE, "rb") as f:
        while data := f.read(CHUNK_SIZE):
            yield datanode_pb2.BlockChunk(
                block_id=BLOCK_ID,
                data=data
            )


# Archivo de prueba de 1 MiB
with open(TEST_FILE, "wb") as f:
    f.write(os.urandom(1024 * 1024))


channel = secure_channel("127.0.0.1:50051")
stub = datanode_pb2_grpc.DataNodeStub(channel)


print("1. PUT")
ack = stub.PutBlock(chunks())
print(ack)


print("2. STAT")
stat = stub.StatBlock(
    datanode_pb2.BlockRequest(block_id=BLOCK_ID)
)
print(stat)


print("3. GET")
with open(RECOVERED_FILE, "wb") as f:
    for chunk in stub.GetBlock(
        datanode_pb2.BlockRequest(block_id=BLOCK_ID)
    ):
        f.write(chunk.data)


original_hash = sha256_file(TEST_FILE)
recovered_hash = sha256_file(RECOVERED_FILE)

print("SHA original :", original_hash)
print("SHA recuperado:", recovered_hash)
print("INTEGRIDAD OK :", original_hash == recovered_hash == stat.sha256)
