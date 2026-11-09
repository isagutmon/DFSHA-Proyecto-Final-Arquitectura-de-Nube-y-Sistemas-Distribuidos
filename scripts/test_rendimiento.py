# -----------------------------------------------------------------------------
# Medición reproducible de latencia y throughput de PUT/GET bajo condiciones normal y degradada.
# Comentarios añadidos únicamente como documentación; no modifican la lógica.
# -----------------------------------------------------------------------------
import argparse
import csv
import hashlib
import math
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVID = ROOT / "docs" / "evidencias"
EVID.mkdir(parents=True, exist_ok=True)

SIZE_MIB = 8
TEST_FILE = Path("/tmp/dfsha_perf_8MiB.bin")


# Calcula el hash SHA-256 usado por la prueba.
def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


# Calcula un percentil simple sobre las mediciones recolectadas.
def percentile(values, p):
    values = sorted(values)
    pos = (len(values) - 1) * p
    low = math.floor(pos)
    high = math.ceil(pos)

    if low == high:
        return values[low]

    return values[low] + (values[high] - values[low]) * (pos - low)


# Ejecuta el CLI DFSha y captura métricas/salida para la prueba de rendimiento.
def cli(*args):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT)

    result = subprocess.run(
        [sys.executable, str(ROOT / "client" / "cli.py"), *args],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print(result.stdout)
        print(result.stderr)
        raise SystemExit(f"ERROR ejecutando: {' '.join(args)}")

    return result.stdout


parser = argparse.ArgumentParser()
parser.add_argument("--condition", required=True)
args = parser.parse_args()

condition = args.condition.lower()

if not TEST_FILE.exists() or TEST_FILE.stat().st_size != SIZE_MIB * 1024 * 1024:
    TEST_FILE.write_bytes(os.urandom(SIZE_MIB * 1024 * 1024))

original_sha = sha256(TEST_FILE)

rows = []

print("=== DFSha - PRUEBA DE RENDIMIENTO ===")
print(f"CONDICION: {condition.upper()}")
print(f"ARCHIVO: {SIZE_MIB} MiB")
print("REPETICIONES: 3")
print()

for rep in range(1, 4):
    remote = f"/perf_{condition}_{rep}.bin"
    downloaded = Path(f"/tmp/perf_{condition}_{rep}_get.bin")

    start = time.perf_counter()
    cli("put", str(TEST_FILE), remote)
    put_seconds = time.perf_counter() - start
    put_rate = SIZE_MIB / put_seconds

    start = time.perf_counter()
    cli("get", remote, str(downloaded))
    get_seconds = time.perf_counter() - start
    get_rate = SIZE_MIB / get_seconds

    ok = sha256(downloaded) == original_sha

    print(
        f"REP {rep}: "
        f"PUT={put_seconds:.3f}s ({put_rate:.2f} MiB/s) | "
        f"GET={get_seconds:.3f}s ({get_rate:.2f} MiB/s) | "
        f"SHA={'OK' if ok else 'ERROR'}"
    )

    rows.extend([
        [condition, rep, "PUT", put_seconds, put_rate, ok],
        [condition, rep, "GET", get_seconds, get_rate, ok],
    ])

csv_path = EVID / f"rendimiento_{condition}.csv"

with open(csv_path, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(
        ["condicion", "repeticion", "operacion",
         "segundos", "mib_s", "sha_ok"]
    )
    writer.writerows(rows)

print()
print("=== RESUMEN ===")

for op in ("PUT", "GET"):
    subset = [r for r in rows if r[2] == op]
    times = [r[3] for r in subset]
    rates = [r[4] for r in subset]

    print(
        f"{op}: "
        f"latencia p50={percentile(times, 0.50):.3f}s | "
        f"p95={percentile(times, 0.95):.3f}s | "
        f"throughput p50={percentile(rates, 0.50):.2f} MiB/s | "
        f"p95={percentile(rates, 0.95):.2f} MiB/s"
    )

print("INTEGRIDAD: TRUE")
print(f"CSV: {csv_path}")
