# -----------------------------------------------------------------------------
# Script de registro de DataNodes en etcd para que el plano de control conozca nodos y endpoints.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import json
from metadata.etcd_store import EtcdStore

NODES = [
    {"node_id": "D1", "endpoint": "172.31.21.156:50051", "status": "active"},
    {"node_id": "D2", "endpoint": "172.31.25.229:50051", "status": "active"},
    {"node_id": "D3", "endpoint": "172.31.28.137:50051", "status": "active"},
]

store = EtcdStore()

for node in NODES:
    store.put(
        f"/dfsha/v1/nodes/{node['node_id']}",
        json.dumps(node),
    )
    print(f"{node['node_id']} -> {node['endpoint']} registrado")

print("REGISTRO DE DATANODES: OK")
