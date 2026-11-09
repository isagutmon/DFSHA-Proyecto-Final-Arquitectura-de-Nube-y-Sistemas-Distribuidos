# -----------------------------------------------------------------------------
# Abstracción del namespace lógico DFSha sobre etcd: rutas, directorios y entradas de archivo.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import json
import posixpath

from metadata.etcd_store import EtcdStore


# Implementa las operaciones del namespace lógico sobre el almacenamiento de metadatos.
class FSStore:
    # Inicializa la instancia con la configuración y dependencias requeridas.
    def __init__(self):
        self.etcd = EtcdStore()

    # Normaliza una ruta lógica para evitar variantes inconsistentes.
    @staticmethod
    def normalize(path: str) -> str:
        if not path:
            return "/"

        if not path.startswith("/"):
            path = "/" + path

        path = posixpath.normpath(path)

        if path == ".":
            return "/"

        return path

    # Obtiene la ruta lógica del directorio padre.
    @staticmethod
    def parent(path: str):
        if path == "/":
            return None

        parent = posixpath.dirname(path.rstrip("/"))
        return parent if parent else "/"

    # Obtiene el nombre base de una ruta lógica.
    @staticmethod
    def base(user_id: str):
        return f"/dfsha/v1/entries/{user_id}"

    # Construye la clave de metadatos usada para una ruta del namespace.
    def key(self, user_id: str, path: str):
        return self.base(user_id) + self.normalize(path)

    # Obtiene la entrada de metadatos asociada a una ruta.
    def get_entry(self, user_id: str, path: str):
        raw = self.etcd.get(self.key(user_id, path))

        if raw is None:
            return None

        return json.loads(raw)

    # Garantiza que exista el directorio raíz del usuario.
    def ensure_root(self, user_id: str):
        if self.get_entry(user_id, "/") is None:
            self.etcd.put(
                self.key(user_id, "/"),
                json.dumps({
                    "path": "/",
                    "type": "dir",
                    "owner": user_id,
                }),
            )

    # Consulta metadatos de una ruta lógica.
    def stat(self, user_id: str, path: str):
        return self.get_entry(user_id, path)

    # Crea un directorio en el namespace lógico.
    def mkdir(self, user_id: str, path: str):
        path = self.normalize(path)

        if path == "/":
            raise ValueError("La raíz ya existe")

        if self.get_entry(user_id, path):
            raise FileExistsError("La ruta ya existe")

        parent = self.get_entry(user_id, self.parent(path))

        if not parent or parent["type"] != "dir":
            raise FileNotFoundError("El directorio padre no existe")

        entry = {
            "path": path,
            "type": "dir",
            "owner": user_id,
        }

        self.etcd.put(
            self.key(user_id, path),
            json.dumps(entry),
        )

        return entry

    # Lista entradas del namespace lógico.
    def ls(self, user_id: str, path: str):
        path = self.normalize(path)

        current = self.get_entry(user_id, path)

        if not current:
            raise FileNotFoundError("Ruta no encontrada")

        if current["type"] != "dir":
            raise NotADirectoryError("La ruta no es un directorio")

        prefix = self.key(user_id, path)

        if not prefix.endswith("/"):
            prefix += "/"

        entries = []

        for _, raw in self.etcd.get_prefix(prefix):
            item = json.loads(raw)

            if item["path"] != path and self.parent(item["path"]) == path:
                entries.append(item)

        return sorted(entries, key=lambda x: x["path"])

    # Elimina un directorio vacío del namespace lógico.
    def rmdir(self, user_id: str, path: str):
        path = self.normalize(path)

        if path == "/":
            raise PermissionError("No se puede eliminar la raíz")

        current = self.get_entry(user_id, path)

        if not current:
            raise FileNotFoundError("Directorio no encontrado")

        if current["type"] != "dir":
            raise NotADirectoryError("La ruta no es un directorio")

        prefix = self.key(user_id, path) + "/"

        if self.etcd.get_prefix(prefix):
            raise OSError("El directorio no está vacío")

        self.etcd.delete(self.key(user_id, path))

    # Elimina un archivo del namespace lógico.
    def rm(self, user_id: str, path: str):
        path = self.normalize(path)

        current = self.get_entry(user_id, path)

        if not current:
            raise FileNotFoundError("Archivo no encontrado")

        if current["type"] != "file":
            raise IsADirectoryError("La ruta es un directorio")

        self.etcd.delete(self.key(user_id, path))
