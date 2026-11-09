# -----------------------------------------------------------------------------
# Prueba de primitivas de lock en etcd: leases y operaciones transaccionales usadas por RF3.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import json
from metadata.etcd_store import EtcdStore

store = EtcdStore()
key = "/dfsha/v1/test/rf3-lock"
value = json.dumps({"owner": "test-rf3"}, sort_keys=True)

store.delete(key)
lease = store.lease_grant(15)

result = store.txn_create_with_lease(
    key,
    value,
    lease,
)

print("TXN CREATE SUCCEEDED:", result.get("succeeded"))
print("LOCK VALUE:", store.get(key))

delete = store.txn_compare_value_delete(key, value)
print("TXN DELETE SUCCEEDED:", delete.get("succeeded"))

store.lease_revoke(lease)
print("ETCD LOCK PRIMITIVES: OK")
