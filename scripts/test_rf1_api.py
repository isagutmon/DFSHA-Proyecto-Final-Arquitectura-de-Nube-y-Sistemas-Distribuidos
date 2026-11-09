# -----------------------------------------------------------------------------
# Prueba de la API RF1: registro, login y operaciones básicas del namespace.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import httpx
import uuid

BASE = "http://127.0.0.1:8443"

suffix = uuid.uuid4().hex[:6]
u1 = f"isabella_{suffix}"
u2 = f"thomas_{suffix}"
password = "DFSha2026!"

# Registra un usuario en el plano de control.
def register(username):
    r = httpx.post(
        f"{BASE}/v1/auth/register",
        json={"username": username, "password": password},
        timeout=5
    )
    r.raise_for_status()

# Autentica al usuario y obtiene la sesión/JWT que usará el cliente.
def login(username):
    r = httpx.post(
        f"{BASE}/v1/auth/login",
        json={"username": username, "password": password},
        timeout=5
    )
    r.raise_for_status()
    return r.json()["access_token"]

# Construye las cabeceras Authorization usadas por la prueba RF1.
def headers(token):
    return {"Authorization": f"Bearer {token}"}

print("=== DFSha - PRUEBA RF1 API ===")

register(u1)
register(u2)

t1 = login(u1)
t2 = login(u2)

print(f"Usuario 1: {u1} -> autenticado")
print(f"Usuario 2: {u2} -> autenticado")

httpx.post(
    f"{BASE}/v1/fs/mkdir",
    json={"path": "/privado_isabella"},
    headers=headers(t1)
).raise_for_status()

httpx.post(
    f"{BASE}/v1/fs/mkdir",
    json={"path": "/privado_thomas"},
    headers=headers(t2)
).raise_for_status()

ls1 = httpx.get(
    f"{BASE}/v1/fs/ls",
    params={"path": "/"},
    headers=headers(t1)
)

ls2 = httpx.get(
    f"{BASE}/v1/fs/ls",
    params={"path": "/"},
    headers=headers(t2)
)

paths1 = [x["path"] for x in ls1.json()["entries"]]
paths2 = [x["path"] for x in ls2.json()["entries"]]

isolation_ok = (
    "/privado_isabella" in paths1
    and "/privado_thomas" not in paths1
    and "/privado_thomas" in paths2
    and "/privado_isabella" not in paths2
)

print("AISLAMIENTO ENTRE USUARIOS:", isolation_ok)

httpx.delete(
    f"{BASE}/v1/fs/rmdir",
    params={"path": "/privado_isabella"},
    headers=headers(t1)
).raise_for_status()

httpx.delete(
    f"{BASE}/v1/fs/rmdir",
    params={"path": "/privado_thomas"},
    headers=headers(t2)
).raise_for_status()

print("MKDIR / LS / RMDIR: OK")
