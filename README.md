# DFSha — Sistema de archivos distribuido

Proyecto final de **Sistemas Distribuidos 2026-2**.  
Equipo: **Isabella Gutiérrez** y **Thomas Osorio Passos**.

DFSha es un prototipo de sistema de archivos distribuido implementado sobre máquinas virtuales de AWS Academy. El sistema separa el plano de control del plano de datos, divide archivos en bloques, mantiene dos réplicas por bloque, usa metadatos compartidos en etcd y permite demostrar concurrencia, tolerancia a fallos, consistencia, seguridad y rendimiento mediante pruebas reproducibles.

---

## 1. Arquitectura implementada

Se adoptó una arquitectura **cliente/servidor con cooperación S2S**, desplegada en tres máquinas virtuales.

```text
                         ┌─────────────────────┐
                         │         CLI         │
                         │ RF1 / RF2 / RF3     │
                         └──────────┬──────────┘
                                    │ HTTPS
                      ┌─────────────┴─────────────┐
                      │                           │
             ┌────────▼────────┐        ┌────────▼────────┐
             │   Control A     │        │   Control B     │
             │     VM1         │        │     VM2         │
             └────────┬────────┘        └────────┬────────┘
                      │          etcd             │
            ┌─────────┴──────────┬───────────────┴─────────┐
            │                    │                         │
       ┌────▼────┐          ┌────▼────┐               ┌────▼────┐
       │ etcd-1  │          │ etcd-2  │               │ etcd-3  │
       │   VM1   │          │   VM2   │               │   VM3   │
       └─────────┘          └─────────┘               └─────────┘

                  gRPC + mTLS + capability temporal
            ┌────────────────┬────────────────┬────────────────┐
            │                │                │
       ┌────▼────┐      ┌────▼────┐      ┌────▼────┐
       │   D1    │      │   D2    │      │   D3    │
       │   VM1   │      │   VM2   │      │   VM3   │
       └─────────┘      └─────────┘      └─────────┘
```

### Topología usada en AWS Academy

| VM | IP privada usada en el laboratorio | Servicios |
|---|---|---|
| VM1 | `172.31.21.156` | Control A, etcd-1, DataNode D1 |
| VM2 | `172.31.25.229` | Control B, etcd-2, DataNode D2 |
| VM3 | `172.31.28.137` | etcd-3, DataNode D3 |

Las IP públicas se usan únicamente para administración por SSH y **no deben codificarse en el cliente**, porque pueden cambiar al reiniciar el laboratorio.

---

## 2. Decisiones principales

- Bloque predeterminado: **4 MiB**.
- Factor de replicación: **R=2** en nodos diferentes.
- Metadatos y coordinación: **etcd de 3 miembros**, con quórum 2/3.
- Dos controles sin estado propio: **Control A y Control B**.
- Plano de control: **REST/HTTPS**.
- Plano de datos: **gRPC sobre TLS con autenticación mutua (mTLS)**.
- Integridad: **SHA-256** por bloque y por archivo.
- Publicación: manifiestos versionados e inmutables.
- Escritura parcial: **copy-on-write (COW)**.
- Concurrencia: **lock exclusivo por archivo** y commit condicionado a la versión esperada.
- Datos temporales: archivos `*.part`, `fsync`, renombrado atómico y limpieza por TTL.
- Autenticación: contraseñas protegidas con **Argon2id**.
- Sesión de usuario: **JWT HS256**.
- Autorización del plano de datos: **capabilities temporales HS256** separadas por operación, bloque y DataNode.
- Cifrado en reposo: EBS cifrado para los datos DFSha.
- El cliente recibe dinámicamente las ubicaciones de los bloques desde el control.

---

## 3. Alcance funcional

### RF1 — Sistema de archivos jerárquico

Comandos implementados:

```text
ls
cd
mkdir
rmdir
rm
```

Cada usuario autenticado dispone de un espacio lógico privado.

### RF2 — Transferencia de archivos completos

```text
put <archivo_local> <ruta_remota>
get <ruta_remota> <archivo_local>
```

`put` divide el archivo en bloques de 4 MiB, distribuye cada bloque en dos DataNodes, verifica las réplicas y publica una versión solamente cuando se alcanza R=2.

`get` obtiene el manifiesto, lee cada bloque desde una réplica disponible, reconstruye el archivo y permite validar el SHA-256 final.

### RF3 — Acceso por rango

```text
open <ruta> [r|rw]
read <offset> <length> [--output ARCHIVO]
lock
write <offset> <archivo_patch>
close
```

Semántica adoptada:

- `open` abre un archivo **existente**.
- `read` permite leer rangos que pueden cruzar varios bloques.
- `write` modifica únicamente bytes existentes.
- `write` **no extiende** el archivo.
- no se implementan `append`, `CREATE` ni `TRUNCATE` dentro de RF3.
- `put` es la operación usada para crear o reemplazar un archivo completo.
- el lock es exclusivo por archivo.
- las modificaciones se realizan mediante COW.
- los lectores fijan una versión/snapshot.

No se pretende implementar semántica POSIX completa.

---

## 4. Requisitos de software

Entorno validado:

- Linux Ubuntu en AWS Academy.
- Python 3.
- Docker para el clúster etcd.
- OpenSSL/certificados para TLS.
- Tres VM comunicadas por red privada.

Dependencias Python utilizadas:

```text
fastapi==0.162.2
uvicorn==0.54.0
grpcio==1.85.1
protobuf==7.36.2
httpx==0.28.1
typer==0.28.0
PyJWT==2.15.1
argon2-cffi==25.1.0
pydantic==2.14.3
```

Instalación:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

---

## 5. Estructura del repositorio

```text
dfsha/
├── client/             # CLI y operaciones RF1/RF2/RF3
├── control/            # API REST/HTTPS y lógica de planes/commit
├── datanode/           # Servicio gRPC de almacenamiento
├── metadata/           # Acceso a etcd y metadatos
├── proto/              # Contratos Protocol Buffers
├── shared/             # Seguridad, TLS, configuración y capabilities
├── scripts/            # Reparación, rendimiento y pruebas reproducibles
├── deploy/             # Configuración de despliegue
├── docs/
│   └── evidencias/     # Resultados de pruebas, CSV y salidas
├── requirements.txt
└── README.md
```

---

## 6. Variables de entorno y secretos

Los secretos **no deben almacenarse en Git**.

En cada VM se utilizó un archivo local:

```text
~/.dfsha_secrets
```

Ejemplo de contenido:

```bash
export DFSHA_CAPABILITY_SECRET="<secreto_aleatorio>"
export DFSHA_JWT_SECRET="<otro_secreto_aleatorio>"
```

Los dos secretos deben ser diferentes.

Para generar valores seguros:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

El mismo `DFSHA_CAPABILITY_SECRET` debe estar disponible en los componentes autorizados que generan o validan capabilities.

El mismo `DFSHA_JWT_SECRET` debe estar configurado en Control A y Control B.

Permisos recomendados:

```bash
chmod 600 ~/.dfsha_secrets
source ~/.dfsha_secrets
```

No existe un secreto de desarrollo por defecto: si `DFSHA_JWT_SECRET` no está definido, el control no debe iniciar correctamente.

---

## 7. Certificados TLS

La carpeta `certs/` debe contener los certificados necesarios para el entorno, pero **las claves privadas no se suben al repositorio**.

Componentes usados:

```text
ca.crt
control-a.crt
control-a.key
control-b.crt
control-b.key
d1.crt / d1.key
d2.crt / d2.key
d3.crt / d3.key
grpc-client.crt / grpc-client.key
```

La CA privada (`ca.key`) debe mantenerse fuera de Git.

El DataNode exige certificado de cliente válido. Una conexión TLS sin certificado de cliente es rechazada.

---

## 8. Configuración del cliente

Ejemplo usado dentro de la VPC:

```bash
export DFSHA_CONTROLS="https://172.31.21.156:8443,https://172.31.25.229:8443"
export DFSHA_CA_CERT="$HOME/dfsha/certs/ca.crt"
```

El cliente conoce únicamente los controles.  
Las ubicaciones de los DataNodes se reciben dinámicamente en los planes de lectura/escritura.

Después del ajuste final, la CLI se ejecuta directamente:

```bash
python client/cli.py --help
```

sin necesidad de definir `PYTHONPATH=.`.

---

## 9. Inicio de los DataNodes

Cargar primero los secretos:

```bash
source ~/.dfsha_secrets
```

### VM1 — D1

```bash
cd ~/dfsha
nohup env \
  PYTHONUNBUFFERED=1 \
  DFSHA_DATANODE_ID=d1 \
  DFSHA_DATA_DIR=/mnt/dfsha-data/d1/blocks \
  DFSHA_DATANODE_PORT=50051 \
  .venv/bin/python -u datanode/server.py \
  > logs/d1.log 2>&1 &
```

### VM2 — D2

```bash
cd ~/dfsha
nohup env \
  PYTHONUNBUFFERED=1 \
  DFSHA_DATANODE_ID=d2 \
  DFSHA_DATA_DIR=/home/ubuntu/dfsha/storage/d2/blocks \
  DFSHA_DATANODE_PORT=50051 \
  .venv/bin/python -u datanode/server.py \
  > logs/d2.log 2>&1 &
```

### VM3 — D3

```bash
cd ~/dfsha
nohup env \
  PYTHONUNBUFFERED=1 \
  DFSHA_DATANODE_ID=d3 \
  DFSHA_DATA_DIR=/home/ubuntu/dfsha/storage/d3/blocks \
  DFSHA_DATANODE_PORT=50051 \
  .venv/bin/python -u datanode/server.py \
  > logs/d3.log 2>&1 &
```

Al iniciar se espera algo equivalente a:

```text
Limpieza staging: 0 temporal(es) eliminado(s) | TTL=300s
DFSha DataNode D1 escuchando con mTLS en puerto 50051
Autenticación mutua TLS: requerida
```

---

## 10. Inicio de los controles

### VM1 — Control A

```bash
cd ~/dfsha
source ~/.dfsha_secrets

nohup env \
  PYTHONUNBUFFERED=1 \
  DFSHA_CONTROL_ID=control-a \
  .venv/bin/python .venv/bin/uvicorn \
  control.main:app \
  --host 0.0.0.0 \
  --port 8443 \
  --ssl-keyfile certs/control-a.key \
  --ssl-certfile certs/control-a.crt \
  > logs/control-a.log 2>&1 &
```

### VM2 — Control B

```bash
cd ~/dfsha
source ~/.dfsha_secrets

nohup env \
  PYTHONUNBUFFERED=1 \
  DFSHA_CONTROL_ID=control-b \
  .venv/bin/python .venv/bin/uvicorn \
  control.main:app \
  --host 0.0.0.0 \
  --port 8443 \
  --ssl-keyfile certs/control-b.key \
  --ssl-certfile certs/control-b.crt \
  > logs/control-b.log 2>&1 &
```

Verificación:

```bash
curl --cacert certs/ca.crt https://172.31.21.156:8443/health
curl --cacert certs/ca.crt https://172.31.25.229:8443/health
```

Resultado esperado:

```json
{"service":"control-a","status":"ok"}
{"service":"control-b","status":"ok"}
```

---

## 11. Clúster etcd

Se utiliza un clúster **etcd de tres miembros**, uno por VM.

Puertos internos:

```text
2379  cliente etcd
2380  comunicación entre miembros
```

Estos puertos deben estar restringidos a la red/SG interno y no publicados a Internet.

La configuración usada en AWS Academy mantiene:

```text
etcd-1 -> VM1
etcd-2 -> VM2
etcd-3 -> VM3
```

Antes de ejecutar DFSha debe verificarse que los tres miembros estén saludables. El catálogo puede mantener quórum con **2 de 3** miembros disponibles.

La configuración de despliegue de etcd se mantiene en `deploy/`. En un nuevo entorno deben ajustarse las IP privadas de los tres miembros antes de iniciar los contenedores.

---

## 12. Uso de la CLI

### Autenticación

```bash
python client/cli.py login <usuario> '<contraseña>'
```

### RF1

```bash
python client/cli.py ls
python client/cli.py mkdir /demo
python client/cli.py cd /demo
python client/cli.py pwd
python client/cli.py cd /
python client/cli.py rmdir /demo
python client/cli.py rm /archivo.txt
```

### RF2

```bash
python client/cli.py put /tmp/original.bin /original.bin
python client/cli.py get /original.bin /tmp/recuperado.bin
sha256sum /tmp/original.bin /tmp/recuperado.bin
```

### RF3

```bash
python client/cli.py open /original.bin r
python client/cli.py read 0 1024 --output /tmp/rango.bin
python client/cli.py close
```

Escritura:

```bash
python client/cli.py open /original.bin rw
python client/cli.py lock
python client/cli.py write 1024 /tmp/patch.bin
python client/cli.py close
```

---

## 13. Seguridad implementada

### Plano de control

- HTTPS.
- autenticación de usuario.
- contraseña protegida con Argon2id.
- JWT firmado.
- autorización por propietario.

### Plano de datos

- gRPC sobre TLS.
- mTLS obligatorio.
- capability temporal por bloque.
- capability ligada a:
  - `block_id`;
  - operación `GET` o `PUT`;
  - `node_id`;
  - tiempo de expiración.

Por ello un cliente con un certificado mTLS válido **no puede leer o escribir arbitrariamente un bloque** si no posee la capability correspondiente.

### Almacenamiento

- VM1: los bloques DFSha se almacenan en un volumen EBS dedicado cifrado montado en `/mnt/dfsha-data`.
- VM2 y VM3: almacenamiento raíz cifrado en el entorno utilizado.
- No se afirma que todo el root de VM1 esté cifrado.

---

## 14. Consistencia, concurrencia y publicación

Una nueva versión se considera visible únicamente después de verificar las condiciones de commit.

Para RF2:

1. Control genera un plan.
2. Cliente envía bloques directamente a los DataNodes.
3. Cada réplica persiste y verifica el bloque.
4. Control comprueba tamaño y SHA-256.
5. Deben existir dos réplicas confirmadas.
6. Se publica el manifiesto/version actual.

Para RF3:

1. `open` fija una versión.
2. `lock` protege el archivo para escritura.
3. `write` genera nuevos bloques mediante COW.
4. los bloques anteriores no se modifican en sitio;
5. el commit publica una nueva versión;
6. un lector con un snapshot anterior puede continuar leyendo su versión.

Los hashes verifican integridad; el lock/CAS coordinan concurrencia. Son mecanismos distintos.

---

## 15. Escrituras interrumpidas

`PutBlock` escribe inicialmente:

```text
<block_id>.part
```

y solo después de:

- completar la transferencia;
- `flush`;
- `fsync`;
- cerrar el archivo;

se ejecuta el renombrado atómico hacia el nombre definitivo.

Si el DataNode muere durante la transferencia, el `.part` no se publica como bloque válido.

Los temporales abandonados se limpian al arrancar cuando superan el TTL configurado:

```text
DFSHA_PART_TTL_SECONDS=300
```

---

## 16. Reparación de réplicas

Script:

```bash
python scripts/repair_replicas.py --help
```

Ejemplo:

```bash
source ~/.dfsha_secrets
python scripts/repair_replicas.py --path /archivo.bin
```

La reparación:

- detecta réplicas no disponibles;
- selecciona una fuente sana;
- usa capability `GET`;
- copia a un nuevo destino con capability `PUT`;
- verifica SHA-256;
- actualiza la lista de réplicas;
- restaura R=2 cuando existe un nodo sano disponible.

---

## 17. Pruebas reproducibles

### Tres DataNodes

```bash
source ~/.dfsha_secrets
python scripts/test_3_datanodes.py
```

Resultado esperado:

```text
[OK] D1 ... PUT+GET autorizado | SHA-256=OK
[OK] D2 ... PUT+GET autorizado | SHA-256=OK
[OK] D3 ... PUT+GET autorizado | SHA-256=OK
RESULTADO: 3/3 DATANODES OPERATIVOS
INFRAESTRUCTURA DE DATOS: OK
```

### Capability denegada

```bash
python scripts/test_capability_rechazo.py
```

Debe producir `PERMISSION_DENIED` cuando se intenta acceder al DataNode con mTLS válido pero sin capability GET.

### Staging interrumpido

```bash
python scripts/test_staging_interrumpido.py
```

La prueba permite detener un DataNode durante una transferencia y comprobar que solo queda un `.part` y que nunca se publica el bloque incompleto.

### Rendimiento

Condición normal:

```bash
python scripts/test_rendimiento.py --condition normal
```

Condición degradada:

```bash
python scripts/test_rendimiento.py --condition degradado
```

Cada condición ejecuta tres repeticiones y registra latencia, throughput, p50/p95 e integridad.

---

## 18. Resultados de rendimiento observados

Archivo utilizado: **8 MiB**.  
Repeticiones por condición: **3**.

| Condición | Operación | Latencia p50 | Latencia p95 | Throughput p50 | Throughput p95 |
|---|---|---:|---:|---:|---:|
| Normal | PUT | 1.429 s | 1.436 s | 5.60 MiB/s | 5.65 MiB/s |
| Normal | GET | 0.496 s | 0.499 s | 16.13 MiB/s | 16.35 MiB/s |
| D1 caído | PUT | 2.832 s | 2.850 s | 2.83 MiB/s | 2.83 MiB/s |
| D1 caído | GET | 0.486 s | 0.492 s | 16.45 MiB/s | 16.64 MiB/s |

En todas las repeticiones el archivo recuperado conservó el SHA correcto.

La disminución del PUT bajo condición degradada fue clara. La pequeña variación observada en GET no se interpreta como una mejora estructural; corresponde a la variabilidad propia de una prueba corta.

---

## 19. Pruebas mínimas cubiertas

| Prueba | Estado |
|---|---|
| Carga y recuperación multibloque + SHA | Cumplida |
| RF3 `read/write` | Cumplida |
| Dos escritores / concurrencia | Cumplida |
| Replicación y caída de DataNode | Cumplida |
| Consistencia con réplica desconectada | Cumplida |
| Acceso autorizado y denegado | Cumplida |
| Protección HTTPS/TLS/mTLS | Cumplida |
| Cifrado en reposo | Cumplida |
| Rendimiento en dos condiciones | Cumplida |
| Escritura interrumpida y staging | Cumplida adicional |
| Reparación R=2 | Cumplida adicional |

Las evidencias detalladas se conservan en la documentación del proyecto.

---

## 20. Limitaciones declaradas

El prototipo no implementa:

- POSIX completo;
- `append`;
- extensión de archivo mediante `write`;
- `CREATE/TRUNCATE` dentro de RF3;
- locks por bloque;
- fencing completo en el DataNode;
- PostgreSQL/Patroni;
- AES-GCM a nivel de aplicación por bloque;
- rotación automática de certificados;
- rebalanceo continuo;
- Kubernetes;
- RabbitMQ/Kafka;
- Prometheus.

Estas funciones no son necesarias para demostrar los requisitos adoptados en la fase grupal y se consideran posibles evoluciones del prototipo.

---

## 21. Archivos que NO deben subirse a Git

El `.gitignore` debe excluir al menos:

```text
.venv/
__pycache__/
*.key
*.pem
.dfsha_secrets
.env
.env.*
session.json
.dfsha/
storage/
logs/
*.log
*.part
```

Antes de `git add`:

```bash
git status --short --ignored
```

y revisar especialmente:

```bash
find . -maxdepth 3 \( -name "*.key" -o -name ".dfsha_secrets" -o -name "session.json" \) -print
```

Nunca subir:

- claves privadas;
- CA privada;
- contraseñas;
- JWT secrets;
- capability secrets;
- archivos de sesión;
- bloques almacenados;
- logs con datos sensibles.

---

## 22. Integridad final del código

Comprobación usada antes del cierre:

```bash
python -m compileall -q client control datanode metadata shared scripts \
  && echo "OK: código Python compila"
```

Auditoría de comunicaciones inseguras:

```bash
grep -RIn \
  --exclude-dir=.venv \
  --exclude='*.bak*' \
  -E 'grpc\.insecure_channel|add_insecure_port|dfsha-development-secret-change-later' \
  client control datanode scripts shared
```

La salida final esperada es vacía.

---

## 23. Contribuciones del equipo

### Isabella Gutiérrez

**Infraestructura e implementación**

- Configuración principal de VM1.
- Despliegue de Control A, etcd-1 y DataNode D1.
- Implementación y validación de RF3.
- Lectura y escritura por rango.
- Copy-on-Write.
- Lock exclusivo por archivo.
- Seguridad mediante mTLS y capabilities temporales.
- Consistencia mediante versiones y snapshots.

**Pruebas y documentación**

- Pruebas de RF3 y concurrencia.
- Pruebas de consistencia.
- Validación de rechazo sin capability.
- Pruebas de staging ante escritura interrumpida.
- Documentación de RF3, seguridad y consistencia.

### Thomas Osorio Passos

**Infraestructura e implementación**

- Configuración principal de VM2 y VM3.
- Despliegue de Control B, etcd-2, etcd-3, DataNode D2 y DataNode D3.
- Implementación y validación de RF1.
- Implementación y validación de RF2.
- Gestión del namespace lógico.
- Transferencia multibloque.
- Replicación R=2.
- Recuperación y reparación de réplicas.

**Pruebas y documentación**

- Pruebas de RF1 y RF2.
- Validación de los tres DataNodes.
- Pruebas de tolerancia a fallos.
- Reparación de réplicas.
- Pruebas de rendimiento en condición normal y degradada.
- Documentación de RF1, RF2, replicación y rendimiento.

### Trabajo conjunto

- Definición de la arquitectura grupal final.
- Integración de Control A y Control B.
- Integración del clúster etcd de tres miembros.
- Integración de D1, D2 y D3.
- Definición del tamaño de bloque y del factor de replicación.
- Integración y validación de RF1, RF2 y RF3.
- Configuración de mecanismos de seguridad.
- Análisis de consistencia, disponibilidad y tolerancia a fallos.
- Validación final del prototipo.
- Elaboración del informe técnico, presentación y demostración en video.

---

## 24. Estado final del prototipo

DFSha fue validado con:

- dos controles HTTPS;
- tres DataNodes gRPC/mTLS;
- tres miembros etcd;
- R=2;
- RF1, RF2 y RF3;
- SHA-256;
- COW;
- snapshots/versionado;
- locks;
- caída y reparación de nodos;
- staging seguro;
- autenticación y autorización;
- capabilities;
- cifrado en tránsito y reposo;
- pruebas de rendimiento repetibles.

El repositorio contiene el prototipo académico y los scripts necesarios para reproducir las demostraciones principales.

---

## 25. Autores

**Isabella Gutiérrez**  
**Thomas Osorio Passos**

Proyecto 1 — Sistemas Distribuidos — 2026-2.
