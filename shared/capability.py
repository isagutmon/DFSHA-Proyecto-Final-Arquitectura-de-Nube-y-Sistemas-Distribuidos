# -----------------------------------------------------------------------------
# Creación y validación de capabilities temporales que autorizan operaciones concretas sobre bloques.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import os
import time
import uuid

import jwt


ALGORITHM = "HS256"
TTL_SECONDS = 60


# Obtiene el secreto HMAC de capabilities desde el entorno; no incorpora valores por defecto inseguros.
def _secret():
    secret = os.getenv("DFSHA_CAPABILITY_SECRET", "").strip()
    if not secret:
        raise RuntimeError(
            "DFSHA_CAPABILITY_SECRET no está configurado"
        )
    return secret


# Crea una capability firmada y limitada por bloque, operación, nodo y expiración.
def create_block_capability(
    block_id: str,
    operation: str,
    node_id: str,
    ttl: int = TTL_SECONDS,
) -> str:
    """
    Permiso temporal para UNA operación sobre UN bloque
    y UN DataNode concreto.
    """
    now = int(time.time())

    payload = {
        "typ": "dfsha-block-capability",
        "block_id": block_id,
        "op": operation.lower(),
        "node_id": node_id.lower(),
        "iat": now,
        "exp": now + ttl,
        "jti": str(uuid.uuid4()),
    }

    return jwt.encode(
        payload,
        _secret(),
        algorithm=ALGORITHM,
    )


# Valida firma, alcance y expiración de una capability antes de autorizar la operación.
def validate_block_capability(
    token: str,
    block_id: str,
    operation: str,
    node_id: str,
):
    payload = jwt.decode(
        token,
        _secret(),
        algorithms=[ALGORITHM],
    )

    if payload.get("typ") != "dfsha-block-capability":
        raise ValueError("Tipo de capacidad inválido")

    if payload.get("block_id") != block_id:
        raise ValueError("Capacidad no válida para este bloque")

    if payload.get("op") != operation.lower():
        raise ValueError("Capacidad no válida para esta operación")

    if payload.get("node_id") != node_id.lower():
        raise ValueError("Capacidad no válida para este DataNode")

    return payload


# Extrae la capability enviada en los metadatos de la llamada gRPC.
def token_from_context(context) -> str:
    metadata = dict(context.invocation_metadata())
    token = metadata.get("x-dfsha-capability")

    if not token:
        raise ValueError("Capacidad temporal requerida")

    return token
