# -----------------------------------------------------------------------------
# Funciones de seguridad del plano de control: Argon2id para contraseñas y JWT para sesiones.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import os
import time
import jwt

from argon2 import PasswordHasher
from argon2.exceptions import VerifyMismatchError


JWT_SECRET = os.getenv("DFSHA_JWT_SECRET", "").strip()

if not JWT_SECRET:
    raise RuntimeError(
        "DFSHA_JWT_SECRET es obligatorio"
    )

JWT_ALGORITHM = "HS256"
JWT_TTL_SECONDS = 3600

ph = PasswordHasher()


# Genera el hash Argon2id utilizado para almacenar contraseñas.
def hash_password(password: str) -> str:
    return ph.hash(password)


# Compara una contraseña recibida con su hash Argon2id almacenado.
def verify_password(password: str, password_hash: str) -> bool:
    try:
        return ph.verify(password_hash, password)
    except VerifyMismatchError:
        return False


# Emite un JWT de sesión con identidad y expiración.
def create_token(user_id: str, username: str) -> str:
    now = int(time.time())

    payload = {
        "sub": user_id,
        "username": username,
        "iat": now,
        "exp": now + JWT_TTL_SECONDS,
    }

    return jwt.encode(
        payload,
        JWT_SECRET,
        algorithm=JWT_ALGORITHM,
    )


# Valida y decodifica un JWT de sesión.
def decode_token(token: str):
    return jwt.decode(
        token,
        JWT_SECRET,
        algorithms=[JWT_ALGORITHM],
    )
