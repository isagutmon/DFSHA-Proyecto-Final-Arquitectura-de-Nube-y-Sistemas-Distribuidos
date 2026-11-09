# -----------------------------------------------------------------------------
# Prueba de consistencia por versiones/snapshots usando el caso TITILAR -> BRILLAR.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import hashlib
import json
import os
import subprocess
import sys

import grpc
import httpx

PROJECT = os.path.expanduser("~/dfsha")
sys.path.insert(0, os.path.join(PROJECT, "datanode"))

import datanode_pb2
import datanode_pb2_grpc
from shared.grpc_tls import secure_channel

BASE = "http://127.0.0.1:8443"
USERNAME = "clientecito"
PASSWORD = "DFSha2026!"

REMOTE_PATH = "/consistencia_estrellita.txt"
LOCAL_PATH = "/tmp/consistencia_estrellita.txt"
RECOVERED_PATH = "/tmp/consistencia_estrellita_final.txt"

OLD_WORD = b"TITILAR"
NEW_WORD = b"BRILLAR"

RPC_CHUNK = 256 * 1024

TEST_TEXT = (
    "ESTRELLITA - PRUEBA DE CONSISTENCIA DFSha\n"
    "========================================\n"
    "Estrellita donde estas,\n"
    "quiero verte titilar,\n"
    "sobre un cielo de prueba,\n"
    "con bloques para verificar.\n"
    "\n"
    "Secuencia numerica: 0123456789\n"
    "Version inicial: 0001\n"
    "Marcador previo: ANTES-COMMIT\n"
    "Mensaje de control: DFSHA-CONSISTENCIA\n"
    "\n"
    "Linea objetivo: quiero verte TITILAR.\n"
    "Marcador final: FIN-ARCHIVO-DFSHA\n"
)

# Realiza peticiones autenticadas al Control dentro de la prueba de consistencia.
def api(method, path, token=None, **kwargs):
    headers = kwargs.pop("headers", {})
    if token:
        headers["Authorization"] = f"Bearer {token}"

    response = httpx.request(
        method,
        BASE + path,
        headers=headers,
        timeout=20,
        **kwargs,
    )

    if response.status_code >= 400:
        raise RuntimeError(
            f"{method} {path}: {response.status_code} {response.text}"
        )

    return response.json()


# Descarga un bloque desde una réplica autorizada y valida el resultado recibido.
def fetch_block(block):
    for replica in block["replicas"]:
        try:
            channel = secure_channel(replica["endpoint"])
            stub = datanode_pb2_grpc.DataNodeStub(channel)

            data = bytearray()

            for chunk in stub.GetBlock(
                datanode_pb2.BlockRequest(
                    block_id=block["block_id"]
                ),
                timeout=20,
            ):
                data.extend(chunk.data)

            candidate = bytes(data)

            if (
                len(candidate) == block["size"]
                and hashlib.sha256(candidate).hexdigest()
                == block["sha256"]
            ):
                return candidate, replica["node_id"]

        except grpc.RpcError:
            continue

    raise RuntimeError(
        f"No se pudo recuperar bloque {block['block_id']}"
    )


# Envía un bloque a un DataNode usando el canal seguro y la capability correspondiente.
def put_block(block_id, data, replica):
    channel = secure_channel(replica["endpoint"])
    stub = datanode_pb2_grpc.DataNodeStub(channel)

    # Genera el stream de mensajes gRPC utilizado en una transferencia de bloque.
    def stream():
        for pos in range(0, len(data), RPC_CHUNK):
            yield datanode_pb2.BlockChunk(
                block_id=block_id,
                data=data[pos:pos + RPC_CHUNK],
            )

    ack = stub.PutBlock(stream(), timeout=20)
    expected = hashlib.sha256(data).hexdigest()

    if (
        not ack.success
        or ack.size != len(data)
        or ack.sha256 != expected
    ):
        raise RuntimeError(
            f"Confirmacion invalida en {replica['node_id']}"
        )


# Lee un rango del handle y reconstruye los bytes desde los bloques planificados.
def read_range(token, handle_id, offset, length):
    plan = api(
        "POST",
        "/v1/files/read-plan",
        token,
        json={
            "handle_id": handle_id,
            "offset": offset,
            "length": length,
        },
    )

    result = bytearray(plan["actual_length"])

    for segment in plan["segments"]:
        block = {
            "block_id": segment["block_id"],
            "size": segment["size"],
            "sha256": segment["sha256"],
            "replicas": segment["replicas"],
        }

        data, _ = fetch_block(block)

        start = segment["block_offset"]
        stop = start + segment["length"]

        out_start = segment["output_offset"]
        out_stop = out_start + segment["length"]

        result[out_start:out_stop] = data[start:stop]

    return bytes(result)


# Cierra el handle de prueba liberando recursos asociados.
def close_handle(token, handle_id):
    try:
        api(
            "POST",
            "/v1/files/close",
            token,
            json={"handle_id": handle_id},
        )
    except Exception:
        pass


print("=== DFSha - PRUEBA DE CONSISTENCIA CON ESTRELLITA ===")

# 1. Crear archivo local controlado.
with open(LOCAL_PATH, "w", encoding="ascii") as f:
    f.write(TEST_TEXT)

original_bytes = TEST_TEXT.encode("ascii")

if original_bytes.count(OLD_WORD) != 1:
    raise RuntimeError(
        "El marcador TITILAR debe aparecer exactamente una vez."
    )

offset = original_bytes.index(OLD_WORD)

print(f"ARCHIVO LOCAL: {LOCAL_PATH}")
print(f"TAMANO ORIGINAL: {len(original_bytes)} bytes")
print(f"OFFSET OBJETIVO: {offset}")
print(f"CAMBIO PREVISTO: {OLD_WORD.decode()} -> {NEW_WORD.decode()}")

# 2. Subir el archivo mediante la CLI real.
subprocess.run(
    [
        sys.executable,
        os.path.join(PROJECT, "client", "cli.py"),
        "login",
        USERNAME,
        PASSWORD,
    ],
    check=True,
)

# Si ya existía de una ejecución anterior, put lo reemplaza.
subprocess.run(
    [
        sys.executable,
        os.path.join(PROJECT, "client", "cli.py"),
        "put",
        LOCAL_PATH,
        REMOTE_PATH,
    ],
    check=True,
)

# 3. Login para operaciones API.
login = api(
    "POST",
    "/v1/auth/login",
    json={
        "username": USERNAME,
        "password": PASSWORD,
    },
)
token = login["access_token"]

writer_id = None
reader_old_id = None
reader_new_id = None

try:
    # 4. Abrir escritor y obtener lock.
    writer = api(
        "POST",
        "/v1/files/open",
        token,
        json={
            "path": REMOTE_PATH,
            "mode": "rw",
        },
    )
    writer_id = writer["handle_id"]

    api(
        "POST",
        "/v1/files/lock",
        token,
        json={"handle_id": writer_id},
    )

    # 5. Abrir lector ANTES de la escritura.
    reader_old = api(
        "POST",
        "/v1/files/open",
        token,
        json={
            "path": REMOTE_PATH,
            "mode": "r",
        },
    )
    reader_old_id = reader_old["handle_id"]

    version_initial = reader_old["version"]

    before = read_range(
        token,
        reader_old_id,
        offset,
        len(OLD_WORD),
    )

    print()
    print(f"VERSION INICIAL: {version_initial}")
    print("LECTURA INICIAL:", before.decode("ascii"))

    # 6. Obtener manifiesto actual y planificar write.
    manifest = api(
        "GET",
        "/v1/files/manifest",
        token,
        params={"path": REMOTE_PATH},
    )

    plan = api(
        "POST",
        "/v1/files/write-plan",
        token,
        json={
            "handle_id": writer_id,
            "offset": offset,
            "size": len(NEW_WORD),
            "patch_sha256": hashlib.sha256(NEW_WORD).hexdigest(),
        },
    )

    changed = {}
    confirmations = []

    # 7. Escribir físicamente nuevos bloques, pero NO hacer commit todavía.
    for item in plan["affected"]:
        old_block = {
            "block_id": item["old_block_id"],
            "size": item["old_size"],
            "sha256": item["old_sha256"],
            "replicas": item["old_replicas"],
        }

        original_block, source_node = fetch_block(old_block)
        modified = bytearray(original_block)

        p_start = item["patch_offset"]
        p_stop = p_start + item["patch_length"]

        b_start = item["block_offset"]
        b_stop = b_start + item["patch_length"]

        modified[b_start:b_stop] = NEW_WORD[p_start:p_stop]
        modified = bytes(modified)

        new_hash = hashlib.sha256(modified).hexdigest()
        stored = []

        for replica in item["new_replicas"]:
            put_block(
                item["new_block_id"],
                modified,
                replica,
            )
            stored.append(replica["node_id"])

        changed[item["index"]] = modified

        confirmations.append({
            "index": item["index"],
            "size": len(modified),
            "sha256": new_hash,
        })

        print(
            f"BLOQUE {item['index']} PREPARADO: "
            f"origen={source_node} | nuevo->{','.join(stored)}"
        )

    print("ESTADO: NUEVOS BLOQUES ESCRITOS, TODAVIA SIN COMMIT")

    # 8. El lector abierto antes debe seguir viendo TITILAR.
    before_commit = read_range(
        token,
        reader_old_id,
        offset,
        len(OLD_WORD),
    )

    print(
        "LECTURA ANTES DEL COMMIT:",
        before_commit.decode("ascii"),
    )

    # 9. Calcular hash esperado de la versión nueva.
    whole_hash = hashlib.sha256()

    for block in sorted(
        manifest["blocks"],
        key=lambda x: x["index"],
    ):
        if block["index"] in changed:
            block_data = changed[block["index"]]
        else:
            block_data, _ = fetch_block(block)

        whole_hash.update(block_data)

    # 10. Publicar commit atómico.
    commit = api(
        "POST",
        f"/v1/writes/{plan['operation_id']}/commit",
        token,
        json={
            "handle_id": writer_id,
            "blocks": confirmations,
            "file_sha256": whole_hash.hexdigest(),
        },
    )

    print(f"COMMIT PUBLICADO: VERSION {commit['version']}")

    # 11. El handle antiguo conserva snapshot.
    old_after_commit = read_range(
        token,
        reader_old_id,
        offset,
        len(OLD_WORD),
    )

    print(
        "HANDLE ANTIGUO DESPUES DEL COMMIT:",
        old_after_commit.decode("ascii"),
    )

    # 12. Nuevo open ve versión publicada nueva.
    reader_new = api(
        "POST",
        "/v1/files/open",
        token,
        json={
            "path": REMOTE_PATH,
            "mode": "r",
        },
    )
    reader_new_id = reader_new["handle_id"]

    after = read_range(
        token,
        reader_new_id,
        offset,
        len(NEW_WORD),
    )

    print(f"NUEVO OPEN: VERSION {reader_new['version']}")
    print("LECTURA NUEVA:", after.decode("ascii"))

    # 13. Descargar versión final completa usando CLI.
    subprocess.run(
        [
            sys.executable,
            os.path.join(PROJECT, "client", "cli.py"),
            "get",
            REMOTE_PATH,
            RECOVERED_PATH,
        ],
        check=True,
    )

    final_bytes = open(RECOVERED_PATH, "rb").read()

    expected_bytes = (
        original_bytes[:offset]
        + NEW_WORD
        + original_bytes[offset + len(OLD_WORD):]
    )

    original_sha = hashlib.sha256(original_bytes).hexdigest()
    expected_sha = hashlib.sha256(expected_bytes).hexdigest()
    final_sha = hashlib.sha256(final_bytes).hexdigest()

    print()
    print("=== VERIFICACION FINAL ===")
    print(f"TAMANO ORIGINAL: {len(original_bytes)} bytes")
    print(f"TAMANO FINAL:    {len(final_bytes)} bytes")
    print(f"SHA ORIGINAL: {original_sha}")
    print(f"SHA ESPERADO: {expected_sha}")
    print(f"SHA FINAL:    {final_sha}")

    ok_before = (
        before == OLD_WORD
        and before_commit == OLD_WORD
    )
    ok_snapshot = old_after_commit == OLD_WORD
    ok_after = (
        after == NEW_WORD
        and reader_new["version"] == commit["version"]
    )
    ok_size = len(final_bytes) == len(original_bytes)
    ok_final = final_bytes == expected_bytes

    print()
    print(
        "VERSION ANTERIOR VISIBLE ANTES DEL COMMIT:",
        str(ok_before).upper(),
    )
    print(
        "HANDLE ANTIGUO CONSERVA SNAPSHOT:",
        str(ok_snapshot).upper(),
    )
    print(
        "NUEVA VERSION VISIBLE TRAS COMMIT:",
        str(ok_after).upper(),
    )
    print(
        "ARCHIVO NO FUE EXTENDIDO:",
        str(ok_size).upper(),
    )
    print(
        "ARCHIVO FINAL EXACTO:",
        str(ok_final).upper(),
    )
    print(
        "PRUEBA DE CONSISTENCIA:",
        str(
            ok_before
            and ok_snapshot
            and ok_after
            and ok_size
            and ok_final
        ).upper(),
    )

finally:
    if reader_new_id:
        close_handle(token, reader_new_id)
    if reader_old_id:
        close_handle(token, reader_old_id)
    if writer_id:
        close_handle(token, writer_id)
