# -----------------------------------------------------------------------------
# Capa de acceso a etcd v3: KV, leases y transacciones CAS usadas por metadatos y coordinación.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import base64
import os
import httpx


DEFAULT_ENDPOINTS = [
    "http://172.31.21.156:2379",
    "http://172.31.25.229:2379",
    "http://172.31.28.137:2379",
]


# Encapsula las operaciones etcd usadas por DFSha para metadatos, leases y transacciones.
class EtcdStore:
    # Inicializa la instancia con la configuración y dependencias requeridas.
    def __init__(self):
        configured = os.getenv("DFSHA_ETCD_ENDPOINTS")
        self.endpoints = configured.split(",") if configured else DEFAULT_ENDPOINTS

    # Codifica valores al formato Base64 esperado por la API HTTP de etcd.
    @staticmethod
    def _b64(value: str) -> str:
        return base64.b64encode(value.encode()).decode()

    # Decodifica valores Base64 recibidos desde etcd.
    @staticmethod
    def _decode(value: str) -> str:
        return base64.b64decode(value).decode()

    # Calcula el límite superior usado para consultas por prefijo en etcd.
    @staticmethod
    def _prefix_end(prefix: str) -> str:
        data = bytearray(prefix.encode())

        for i in range(len(data) - 1, -1, -1):
            if data[i] < 255:
                data[i] += 1
                return base64.b64encode(bytes(data[:i + 1])).decode()

        return base64.b64encode(b"\0").decode()

    # Realiza una petición a la API v3 de etcd y centraliza el manejo de errores.
    def _post(self, path: str, payload: dict):
        last_error = None

        for endpoint in self.endpoints:
            try:
                response = httpx.post(
                    endpoint + path,
                    json=payload,
                    timeout=5.0,
                )
                response.raise_for_status()
                return response.json()
            except Exception as exc:
                last_error = exc

        raise RuntimeError(f"No hay endpoint etcd disponible: {last_error}")

    # Sube datos/bloques al sistema siguiendo el plan emitido por el Control.
    def put(self, key: str, value: str):
        return self._post(
            "/v3/kv/put",
            {
                "key": self._b64(key),
                "value": self._b64(value),
            },
        )

    # Guarda una clave asociándola a un lease temporal.
    def put_with_lease(self, key: str, value: str, lease_id):
        return self._post(
            "/v3/kv/put",
            {
                "key": self._b64(key),
                "value": self._b64(value),
                "lease": str(lease_id),
            },
        )

    # Recupera datos/bloques y reconstruye el archivo solicitado.
    def get(self, key: str):
        result = self._post(
            "/v3/kv/range",
            {"key": self._b64(key)},
        )

        kvs = result.get("kvs", [])

        if not kvs:
            return None

        return self._decode(kvs[0]["value"])

    # Recupera todas las claves bajo un prefijo determinado.
    def get_prefix(self, prefix: str):
        result = self._post(
            "/v3/kv/range",
            {
                "key": self._b64(prefix),
                "range_end": self._prefix_end(prefix),
            },
        )

        return [
            (
                self._decode(item["key"]),
                self._decode(item["value"]),
            )
            for item in result.get("kvs", [])
        ]

    # Elimina una clave de etcd.
    def delete(self, key: str):
        return self._post(
            "/v3/kv/deleterange",
            {"key": self._b64(key)},
        )

    # Solicita a etcd un lease con TTL.
    def lease_grant(self, ttl: int = 60):
        result = self._post(
            "/v3/lease/grant",
            {"TTL": ttl},
        )
        return result["ID"]

    # Revoca un lease y elimina las claves temporales asociadas.
    def lease_revoke(self, lease_id):
        try:
            return self._post(
                "/v3/lease/revoke",
                {"ID": str(lease_id)},
            )
        except Exception:
            return None

    # Crea una clave solo si no existe, asociándola a un lease; se usa para exclusión mutua.
    def txn_create_with_lease(
        self,
        key: str,
        value: str,
        lease_id,
    ):
        return self._post(
            "/v3/kv/txn",
            {
                "compare": [
                    {
                        "target": "VERSION",
                        "key": self._b64(key),
                        "version": "0",
                        "result": "EQUAL",
                    }
                ],
                "success": [
                    {
                        "requestPut": {
                            "key": self._b64(key),
                            "value": self._b64(value),
                            "lease": str(lease_id),
                        }
                    }
                ],
                "failure": [
                    {
                        "requestRange": {
                            "key": self._b64(key),
                        }
                    }
                ],
            },
        )

    # Actualiza una clave únicamente si mantiene el valor esperado (CAS).
    def txn_compare_value_put(
        self,
        key: str,
        expected_value: str,
        new_value: str,
        lease_id=None,
    ):
        put_request = {
            "key": self._b64(key),
            "value": self._b64(new_value),
        }

        if lease_id is not None:
            put_request["lease"] = str(lease_id)

        return self._post(
            "/v3/kv/txn",
            {
                "compare": [
                    {
                        "target": "VALUE",
                        "key": self._b64(key),
                        "value": self._b64(expected_value),
                        "result": "EQUAL",
                    }
                ],
                "success": [
                    {"requestPut": put_request}
                ],
                "failure": [],
            },
        )

    # Elimina una clave únicamente si mantiene el valor esperado.
    def txn_compare_value_delete(
        self,
        key: str,
        expected_value: str,
    ):
        return self._post(
            "/v3/kv/txn",
            {
                "compare": [
                    {
                        "target": "VALUE",
                        "key": self._b64(key),
                        "value": self._b64(expected_value),
                        "result": "EQUAL",
                    }
                ],
                "success": [
                    {
                        "requestDeleteRange": {
                            "key": self._b64(key),
                        }
                    }
                ],
                "failure": [],
            },
        )

    # Actualiza varias claves de forma transaccional cuando se cumple la comparación esperada.
    def txn_compare_value_multi_put(
        self,
        compare_key: str,
        expected_value: str,
        puts: list[tuple[str, str]],
    ):
        success = []

        for key, value in puts:
            success.append(
                {
                    "requestPut": {
                        "key": self._b64(key),
                        "value": self._b64(value),
                    }
                }
            )

        return self._post(
            "/v3/kv/txn",
            {
                "compare": [
                    {
                        "target": "VALUE",
                        "key": self._b64(compare_key),
                        "value": self._b64(expected_value),
                        "result": "EQUAL",
                    }
                ],
                "success": success,
                "failure": [],
            },
        )

    # Expone el estado básico del servicio de control.
    def health(self):
        results = []

        for endpoint in self.endpoints:
            try:
                response = httpx.get(endpoint + "/health", timeout=2.0)
                healthy = (
                    response.status_code == 200
                    and response.json().get("health") == "true"
                )
            except Exception:
                healthy = False

            results.append({
                "endpoint": endpoint,
                "healthy": healthy,
            })

        return results
