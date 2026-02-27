"""
create_delta_data.py
────────────────────
Escribe datos de transacciones en dos destinos:

  MODO ADLS (por defecto — Parte 3 completa):
    1. Tabla Delta Lake con UniForm/Iceberg en ADLS Gen2 (Azure)
       → BigQuery la lee como tabla externa ICEBERG vía BigQuery Omni
    2. Snapshot Parquet en GCS como respaldo para tabla externa PARQUET

  MODO GCS (fallback — si no hay credenciales Azure):
    1. Tabla Delta Lake en GCS
    2. Snapshot Parquet en GCS para tabla externa PARQUET en BigQuery

Variables de entorno:
  ADLS_ACCOUNT_NAME   Cuenta de Azure Storage (ej: jaredpruebadelta)
  ADLS_ACCESS_KEY     Clave de acceso de Azure Storage
  ADLS_CONTAINER      Contenedor ADLS Gen2 (ej: datalake)
  ADLS_DELTA_PATH     Ruta dentro del contenedor (ej: transactions_uniform)
  GCS_BUCKET          Bucket GCS para snapshot Parquet
  GOOGLE_APPLICATION_CREDENTIALS  SA key JSON para GCS

Nota IA: Generado con asistencia de Claude (Anthropic).
"""

import logging
import os
import sys
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Configuración ADLS Gen2 ────────────────────────────────────────────────────
ADLS_ACCOUNT   = os.environ.get("ADLS_ACCOUNT_NAME", "jaredpruebadelta")
ADLS_KEY       = os.environ.get("ADLS_ACCESS_KEY",   "")
ADLS_CONTAINER = os.environ.get("ADLS_CONTAINER",    "datalake")
ADLS_PATH      = os.environ.get("ADLS_DELTA_PATH",   "transactions_uniform")

ADLS_URI = f"abfss://{ADLS_CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/{ADLS_PATH}"

# ── Configuración GCS (snapshot Parquet para BigQuery) ─────────────────────────
GCS_BUCKET    = os.environ.get("GCS_BUCKET",  "raw-dev-michaelpage-prueba")
GCS_DELTA_PATH  = os.environ.get("GCS_DELTA_PATH",   "delta/transactions")
GCS_PARQUET_PATH = "parquet/transactions"
GCS_DELTA_URI   = f"gs://{GCS_BUCKET}/{GCS_DELTA_PATH}"
SA_KEY_PATH     = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")


# ── Datos de muestra ───────────────────────────────────────────────────────────
def build_sample_data() -> pa.Table:
    """Datos iniciales — 5 transacciones (versión 0 del Delta)."""
    return pa.table({
        "transaction_id":   pa.array(["TXN-001", "TXN-002", "TXN-003", "TXN-004", "TXN-005"]),
        "customer_id":      pa.array(["CUST-A",  "CUST-B",  "CUST-A",  "CUST-C",  "CUST-B"]),
        "amount":           pa.array(
                                [Decimal("150.00"), Decimal("220.50"), Decimal("89.99"),
                                 Decimal("310.00"), Decimal("45.75")],
                                type=pa.decimal128(18, 2)),
        "transaction_date": pa.array(["2024-01-15", "2024-01-16", "2024-01-17",
                                       "2024-01-18", "2024-01-19"]),
        "status":           pa.array(["completed", "completed", "refunded",
                                       "completed", "pending"]),
    })


def build_incremental_data() -> pa.Table:
    """Datos incrementales — simula nueva carga (versión 1 del Delta)."""
    return pa.table({
        "transaction_id":   pa.array(["TXN-003", "TXN-006", "TXN-007"]),
        "customer_id":      pa.array(["CUST-A",  "CUST-D",  "CUST-C"]),
        "amount":           pa.array(
                                [Decimal("89.99"), Decimal("500.00"), Decimal("75.25")],
                                type=pa.decimal128(18, 2)),
        "transaction_date": pa.array(["2024-01-17", "2024-01-20", "2024-01-21"]),
        "status":           pa.array(["completed", "completed", "completed"]),
    })


# ── MODO ADLS Gen2 (principal) ─────────────────────────────────────────────────
def get_adls_storage_options() -> dict:
    """
    Opciones de autenticación para delta-rs con ADLS Gen2.
    Usa account key directamente — funciona sin Databricks ni Unity Catalog.
    """
    if not ADLS_KEY:
        raise EnvironmentError(
            "ADLS_ACCESS_KEY no definida. "
            "Configura la variable de entorno con la clave de tu Storage Account."
        )
    return {
        "ACCOUNT_NAME": ADLS_ACCOUNT,
        "ACCESS_KEY":   ADLS_KEY,
    }


def write_delta_to_adls() -> pa.Table:
    """
    Escribe tabla Delta Lake en ADLS Gen2 con UniForm habilitado.

    UniForm genera metadatos Iceberg automáticamente en cada commit,
    lo que permite que BigQuery Omni lea la tabla como EXTERNAL TABLE ICEBERG
    apuntando directamente a ADLS Gen2 — sin copiar datos a GCP.
    """
    storage_opts = get_adls_storage_options()

    log.info("📂 Destino ADLS Gen2: %s", ADLS_URI)
    log.info("   Cuenta:     %s", ADLS_ACCOUNT)
    log.info("   Contenedor: %s", ADLS_CONTAINER)
    log.info("   Ruta:       %s", ADLS_PATH)

    # Versión 0 — datos iniciales
    initial_data = build_sample_data()
    write_deltalake(
        table_or_uri    = ADLS_URI,
        data            = initial_data,
        mode            = "overwrite",
        storage_options = storage_opts,
        # UniForm: genera metadatos Iceberg en cada commit
        # BigQuery Omni los lee con format='ICEBERG' y WITH CONNECTION
        configuration   = {
            "delta.universalFormat.enabledFormats": "iceberg",
            "delta.enableIcebergCompatV2":          "true",
        },
    )
    log.info("✅ Versión 0 Delta en ADLS — %d filas", initial_data.num_rows)

    # Versión 1 — datos incrementales
    incremental_data = build_incremental_data()
    write_deltalake(
        table_or_uri    = ADLS_URI,
        data            = incremental_data,
        mode            = "append",
        storage_options = storage_opts,
    )
    log.info("✅ Versión 1 Delta en ADLS — %d filas adicionales", incremental_data.num_rows)

    # Verificar estado final
    dt = DeltaTable(ADLS_URI, storage_options=storage_opts)
    full_table = dt.to_pyarrow_table()
    log.info("📊 Delta verificada — versión: %d | archivos: %d | filas totales: %d",
             dt.version(), len(dt.files()), full_table.num_rows)

    log.info("")
    log.info("🔗 Para crear la tabla externa en BigQuery ejecuta:")
    log.info("   CREATE OR REPLACE EXTERNAL TABLE `dw_dev_omni.transactions_federated`")
    log.info("   WITH CONNECTION `projects/.../locations/azure-eastus2/connections/adls-biglake-conn`")
    log.info("   OPTIONS (")
    log.info("     format = 'DELTA_LAKE',")
    log.info("     uris   = ['azure://jaredpruebadelta.blob.core.windows.net/datalake/transactions_uniform/']")
    log.info("   );")

    # Exportar snapshot Parquet a GCS para que refresh_biglake.py
    # pueda crear la tabla externa de respaldo en dw_dev
    log.info("📤 Exportando snapshot Parquet a GCS (tabla BigLake de respaldo en dw_dev)...")
    write_parquet_snapshot_to_gcs(full_table)

    return full_table


# ── MODO GCS (fallback) ────────────────────────────────────────────────────────
def get_gcs_storage_options() -> dict:
    if not SA_KEY_PATH or not os.path.exists(SA_KEY_PATH):
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS no definido o no existe.")
    return {"google_service_account": SA_KEY_PATH}


def write_parquet_snapshot_to_gcs(table: pa.Table) -> None:
    """Escribe snapshot Parquet en GCS para tabla externa PARQUET en BigQuery."""
    import tempfile
    from google.cloud import storage as gcs

    gcs_client = gcs.Client()
    bucket = gcs_client.bucket(GCS_BUCKET)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    pq.write_table(table, tmp_path)
    blob = bucket.blob(f"{GCS_PARQUET_PATH}/transactions.parquet")
    blob.upload_from_filename(tmp_path)
    os.unlink(tmp_path)

    log.info("✅ Snapshot Parquet en GCS: gs://%s/%s/transactions.parquet",
             GCS_BUCKET, GCS_PARQUET_PATH)


def write_delta_to_gcs() -> pa.Table:
    """Fallback: escribe Delta en GCS cuando no hay credenciales Azure."""
    storage_opts = get_gcs_storage_options()

    log.info("📂 Destino GCS (fallback): %s", GCS_DELTA_URI)

    initial_data = build_sample_data()
    write_deltalake(GCS_DELTA_URI, initial_data, mode="overwrite",
                    storage_options=storage_opts)
    log.info("✅ Versión 0 Delta en GCS — %d filas", initial_data.num_rows)

    incremental_data = build_incremental_data()
    write_deltalake(GCS_DELTA_URI, incremental_data, mode="append",
                    storage_options=storage_opts)
    log.info("✅ Versión 1 Delta en GCS — %d filas adicionales", incremental_data.num_rows)

    dt = DeltaTable(GCS_DELTA_URI, storage_options=storage_opts)
    full_table = dt.to_pyarrow_table()
    log.info("📊 Delta verificada — versión: %d | archivos: %d",
             dt.version(), len(dt.files()))

    write_parquet_snapshot_to_gcs(full_table)
    return full_table


# ── Entry point ────────────────────────────────────────────────────────────────
def main() -> None:
    use_adls = bool(ADLS_KEY)

    if use_adls:
        log.info("🚀 Modo ADLS Gen2 — escritura Delta con UniForm/Iceberg en Azure")
        try:
            write_delta_to_adls()
        except Exception as exc:
            log.error("❌ Error escribiendo en ADLS Gen2: %s", exc)
            sys.exit(1)
    else:
        log.info("🚀 Modo GCS (fallback) — ADLS_ACCESS_KEY no definida")
        try:
            write_delta_to_gcs()
        except Exception as exc:
            log.error("❌ Error escribiendo en GCS: %s", exc)
            sys.exit(1)


if __name__ == "__main__":
    main()