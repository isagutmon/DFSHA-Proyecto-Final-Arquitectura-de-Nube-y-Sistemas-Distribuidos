# -----------------------------------------------------------------------------
# API principal del plano de control: autenticación, salud del servicio y operaciones RF1.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import json
import os
import uuid

from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

from metadata.etcd_store import EtcdStore
from metadata.fs_store import FSStore
from shared.security import (
    create_token,
    decode_token,
    hash_password,
    verify_password,
)


CONTROL_ID = os.getenv("DFSHA_CONTROL_ID", "control-a")

app = FastAPI(
    title="DFSha Control API",
    version="0.2.0",
)

etcd = EtcdStore()
fs = FSStore()
bearer = HTTPBearer(auto_error=False)


# Modelo de credenciales usado para registro y autenticación.
class Credentials(BaseModel):
    username: str = Field(min_length=3, max_length=50)
    password: str = Field(min_length=8, max_length=128)


# Modelo simple para operaciones RF1 que reciben una ruta.
class PathRequest(BaseModel):
    path: str


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
        raise HTTPException(status_code=401, detail="Token inválido o expirado")


# Expone el estado básico del servicio de control.
@app.get("/health")
def health():
    return {
        "service": CONTROL_ID,
        "status": "ok",
    }


# Comprueba la disponibilidad de etcd utilizada por el Control.
@app.get("/v1/system/etcd")
def etcd_status():
    endpoints = etcd.health()
    healthy_count = sum(1 for x in endpoints if x["healthy"])

    return {
        "control": CONTROL_ID,
        "healthy_members": healthy_count,
        "total_members": len(endpoints),
        "quorum_available": healthy_count >= 2,
        "members": endpoints,
    }


# Registra un usuario en el plano de control.
@app.post("/v1/auth/register")
def register(data: Credentials):
    username_key = f"/dfsha/v1/usernames/{data.username}"

    if etcd.get(username_key):
        raise HTTPException(status_code=409, detail="Usuario ya existe")

    user_id = str(uuid.uuid4())

    user = {
        "user_id": user_id,
        "username": data.username,
        "password_hash": hash_password(data.password),
    }

    etcd.put(
        f"/dfsha/v1/users/{user_id}",
        json.dumps(user),
    )

    etcd.put(username_key, user_id)

    fs.ensure_root(user_id)

    return {
        "user_id": user_id,
        "username": data.username,
    }


# Autentica al usuario y obtiene la sesión/JWT que usará el cliente.
@app.post("/v1/auth/login")
def login(data: Credentials):
    user_id = etcd.get(f"/dfsha/v1/usernames/{data.username}")

    if not user_id:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    raw = etcd.get(f"/dfsha/v1/users/{user_id}")

    if not raw:
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    user = json.loads(raw)

    if not verify_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Credenciales inválidas")

    return {
        "access_token": create_token(user_id, data.username),
        "token_type": "bearer",
    }


# Lista entradas del namespace lógico.
@app.get("/v1/fs/ls")
def ls(path: str = "/", user=Depends(current_user)):
    try:
        return {
            "path": fs.normalize(path),
            "entries": fs.ls(user["user_id"], path),
        }
    except (FileNotFoundError, NotADirectoryError) as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# Consulta metadatos de una ruta lógica.
@app.get("/v1/fs/stat")
def stat(path: str, user=Depends(current_user)):
    entry = fs.stat(user["user_id"], path)

    if not entry:
        raise HTTPException(status_code=404, detail="Ruta no encontrada")

    return entry


# Crea un directorio en el namespace lógico.
@app.post("/v1/fs/mkdir")
def mkdir(data: PathRequest, user=Depends(current_user)):
    try:
        return fs.mkdir(user["user_id"], data.path)
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail=str(exc))
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))


# Elimina un directorio vacío del namespace lógico.
@app.delete("/v1/fs/rmdir")
def rmdir(path: str, user=Depends(current_user)):
    try:
        fs.rmdir(user["user_id"], path)
        return {"deleted": fs.normalize(path)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except (PermissionError, OSError) as exc:
        raise HTTPException(status_code=409, detail=str(exc))


# Elimina un archivo del namespace lógico.
@app.delete("/v1/fs/rm")
def rm(path: str, user=Depends(current_user)):
    try:
        fs.rm(user["user_id"], path)
        return {"deleted": fs.normalize(path)}
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except IsADirectoryError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

from control.rf2 import router as rf2_router
app.include_router(rf2_router)

from control.rf3 import router as rf3_router
app.include_router(rf3_router)
