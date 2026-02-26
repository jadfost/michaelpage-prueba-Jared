"""
create_delta_data.py
────────────────────
Escribe datos de transacciones en GCS:
  1. Tabla Delta Lake completa (para versionado / auditoría)
  2. Snapshot Parquet (para tabla externa BigQuery)

Nota IA: Estructura base generada con asistencia de Claude (Anthropic).
"""

import logging
import os
import sys
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, write_deltalake
from google.cloud import storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

GCS_BUCKET  = os.environ.get("GCS_BUCKET", "raw-dev-michaelpage-prueba")
DELTA_PATH  = os.environ.get("DELTA_PATH", "delta/transactions")
PARQUET_PATH = "parquet/transactions"
TABLE_URI   = f"gs://{GCS_BUCKET}/{DELTA_PATH}"
SA_KEY_PATH = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")


def get_storage_options() -> dict:
    if not SA_KEY_PATH or not os.path.exists(SA_KEY_PATH):
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS no definido o no existe.")
    return {"google_service_account": SA_KEY_PATH}


def build_sample_data() -> pa.Table:
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
    return pa.table({
        "transaction_id":   pa.array(["TXN-003", "TXN-006", "TXN-007"]),
        "customer_id":      pa.array(["CUST-A",  "CUST-D",  "CUST-C"]),
        "amount":           pa.array(
                                [Decimal("89.99"), Decimal("500.00"), Decimal("75.25")],
                                type=pa.decimal128(18, 2)),
        "transaction_date": pa.array(["2024-01-17", "2024-01-20", "2024-01-21"]),
        "status":           pa.array(["completed", "completed", "completed"]),
    })


def write_parquet_snapshot_to_gcs(table: pa.Table) -> None:
    """Escribe un snapshot Parquet en GCS para la tabla externa de BigQuery."""
    import tempfile, os
    gcs_client = storage.Client()
    bucket = gcs_client.bucket(GCS_BUCKET)

    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
        tmp_path = tmp.name

    pq.write_table(table, tmp_path)

    blob = bucket.blob(f"{PARQUET_PATH}/transactions.parquet")
    blob.upload_from_filename(tmp_path)
    os.unlink(tmp_path)

    log.info("✅ Snapshot Parquet subido: gs://%s/%s/transactions.parquet",
             GCS_BUCKET, PARQUET_PATH)


def write_delta_to_gcs() -> None:
    storage_opts = get_storage_options()
    log.info("Escribiendo tabla Delta en: %s", TABLE_URI)

    initial_data = build_sample_data()
    write_deltalake(TABLE_URI, initial_data, mode="overwrite", storage_options=storage_opts)
    log.info("✅ Versión 0 Delta escrita — %d filas", initial_data.num_rows)

    incremental_data = build_incremental_data()
    write_deltalake(TABLE_URI, incremental_data, mode="append", storage_options=storage_opts)
    log.info("✅ Versión 1 Delta escrita — %d filas adicionales", incremental_data.num_rows)

    dt = DeltaTable(TABLE_URI, storage_options=storage_opts)
    log.info("📊 Delta verificada — versión: %d | archivos: %d", dt.version(), len(dt.files()))

    # Leer el estado actual de Delta y exportar a Parquet para BigQuery
    full_table = dt.to_pyarrow_table()
    write_parquet_snapshot_to_gcs(full_table)


def main() -> None:
    try:
        write_delta_to_gcs()
    except Exception as exc:
        log.error("Error: %s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()