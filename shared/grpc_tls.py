# -----------------------------------------------------------------------------
# Configuración TLS/mTLS compartida para canales y servidores gRPC.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import os
from functools import lru_cache
from pathlib import Path

import grpc


PROJECT_ROOT = Path(__file__).resolve().parents[1]

CERT_DIR = Path(
    os.getenv(
        "DFSHA_CERT_DIR",
        str(PROJECT_ROOT / "certs"),
    )
)

CA_CERT = Path(
    os.getenv(
        "DFSHA_GRPC_CA_CERT",
        str(CERT_DIR / "ca.crt"),
    )
)

CLIENT_CERT = Path(
    os.getenv(
        "DFSHA_GRPC_CLIENT_CERT",
        str(CERT_DIR / "grpc-client.crt"),
    )
)

CLIENT_KEY = Path(
    os.getenv(
        "DFSHA_GRPC_CLIENT_KEY",
        str(CERT_DIR / "grpc-client.key"),
    )
)


# Carga certificados y construye las credenciales mTLS del cliente gRPC.
@lru_cache(maxsize=1)
def client_credentials():
    """
    Credenciales mTLS usadas por CLI, controles, reparación y pruebas.

    El cliente:
    1. valida el certificado del DataNode con ca.crt;
    2. presenta grpc-client.crt;
    3. demuestra posesión de grpc-client.key.
    """
    return grpc.ssl_channel_credentials(
        root_certificates=CA_CERT.read_bytes(),
        private_key=CLIENT_KEY.read_bytes(),
        certificate_chain=CLIENT_CERT.read_bytes(),
    )


# Crea un canal gRPC seguro hacia el endpoint indicado.
def secure_channel(endpoint: str):
    """Crea un canal gRPC autenticado y cifrado mediante mTLS."""
    return grpc.secure_channel(
        endpoint,
        client_credentials(),
    )


# Carga certificados del DataNode y exige certificado válido del cliente.
def server_credentials(node_id: str):
    """
    Credenciales del DataNode.

    Cada DataNode utiliza su certificado propio:
      d1 -> d1.crt / d1.key
      d2 -> d2.crt / d2.key
      d3 -> d3.crt / d3.key

    require_client_auth=True obliga al cliente a presentar
    un certificado firmado por la CA de DFSha.
    """
    cert_path = Path(
        os.getenv(
            "DFSHA_GRPC_SERVER_CERT",
            str(CERT_DIR / f"{node_id}.crt"),
        )
    )

    key_path = Path(
        os.getenv(
            "DFSHA_GRPC_SERVER_KEY",
            str(CERT_DIR / f"{node_id}.key"),
        )
    )

    return grpc.ssl_server_credentials(
        [(key_path.read_bytes(), cert_path.read_bytes())],
        root_certificates=CA_CERT.read_bytes(),
        require_client_auth=True,
    )
