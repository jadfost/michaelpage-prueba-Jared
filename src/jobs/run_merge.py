"""
run_merge.py
────────────
Orquesta el flujo completo de la Parte 4 usando Python ETL Bridge:

  ADLS Gen2 (Delta Lake)
       │
       │  deltalake + PyArrow  — lectura directa del Delta log
       ▼
  PyArrow Table en memoria
       │
       │  google-cloud-bigquery  — load_table_from_dataframe
       ▼
  dw_{env}.transactions_staging  (BigQuery US, tabla nativa)
       │
       │  MERGE SQL  (BigQuery US, sin restricciones cross-region)
       ▼
  dw_{env}.final_table  (enriquecida con customers)

Ventajas frente a BigQuery Omni Export → GCS:
  - No depende de permisos cross-cloud (GCS ← azure-eastus2).
  - Lee siempre la última versión del Delta log directamente.
  - Un solo salto: ADLS Gen2 → BigQuery (sin GCS intermedio).
  - Totalmente idempotente (WRITE_TRUNCATE en staging).

Uso:
    python run_merge.py \
        --project        michaelpage-prueba  \
        --dataset        dw_dev              \
        --env            dev                 \
        --adls-account   jaredpruebadelta    \
        --adls-container datalake            \
        --adls-path      transactions_uniform

Variables de entorno requeridas:
    ADLS_ACCESS_KEY                — clave de acceso ADLS Gen2
    GOOGLE_APPLICATION_CREDENTIALS — path al JSON de la SA GCP

Nota IA: Refactorizado con asistencia de Claude (Anthropic).
"""

import argparse
import logging
import os
import sys

import pyarrow as pa
import pandas as pd
from deltalake import DeltaTable
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# DDL: tabla nativa de clientes (maestro) en US
# ──────────────────────────────────────────────────────────────
CREATE_CUSTOMERS_SQL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.customers` (
  customer_id   STRING  NOT NULL,
  customer_name STRING,
  email         STRING,
  country       STRING,
  updated_at    TIMESTAMP,
  PRIMARY KEY (customer_id) NOT ENFORCED
)
OPTIONS (
  labels = [
    ('environment',    '{env}'),
    ('owner',          'data-team'),
    ('domain',         'commerce'),
    ('classification', 'internal')
  ]
)
"""

INSERT_CUSTOMERS_SQL = """
INSERT INTO `{project}.{dataset}.customers`
  (customer_id, customer_name, email, country, updated_at)
SELECT * FROM UNNEST([
  STRUCT('CUST-A' AS customer_id, 'Alice Johnson'  AS customer_name, 'alice@example.com'  AS email, 'Colombia' AS country, CURRENT_TIMESTAMP() AS updated_at),
  STRUCT('CUST-B',                'Bob Smith',       'bob@example.com',   'Mexico',   CURRENT_TIMESTAMP()),
  STRUCT('CUST-C',                'Carlos Rivera',   'carlos@example.com','Colombia', CURRENT_TIMESTAMP()),
  STRUCT('CUST-D',                'Diana Torres',    'diana@example.com', 'Peru',     CURRENT_TIMESTAMP())
])
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.{dataset}.customers` LIMIT 1
)
"""

# ──────────────────────────────────────────────────────────────
# DDL: tabla destino del MERGE en US
# ──────────────────────────────────────────────────────────────
CREATE_FINAL_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS `{project}.{dataset}.final_table` (
  transaction_id   STRING  NOT NULL,
  customer_id      STRING,
  customer_name    STRING,
  country          STRING,
  amount           NUMERIC,
  transaction_date DATE,
  status           STRING,
  last_updated     TIMESTAMP,
  PRIMARY KEY (transaction_id) NOT ENFORCED
)
OPTIONS (
  labels = [
    ('environment',    '{env}'),
    ('owner',          'data-team'),
    ('domain',         'commerce'),
    ('classification', 'internal')
  ]
)
"""

# ──────────────────────────────────────────────────────────────
# MERGE: transactions_staging + customers → final_table (US)
# ──────────────────────────────────────────────────────────────
MERGE_SQL = """
MERGE `{project}.{dataset}.final_table` AS target
USING (
  SELECT
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    c.country,
    CAST(t.amount AS NUMERIC)  AS amount,
    DATE(t.transaction_date)   AS transaction_date,
    t.status,
    CURRENT_TIMESTAMP()        AS last_updated
  FROM `{project}.{dataset}.transactions_staging` AS t
  LEFT JOIN `{project}.{dataset}.customers`        AS c
    ON t.customer_id = c.customer_id
) AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED AND (
  target.status        != source.status        OR
  target.customer_name != source.customer_name OR
  target.amount        != source.amount
) THEN UPDATE SET
  customer_name = source.customer_name,
  country       = source.country,
  amount        = source.amount,
  status        = source.status,
  last_updated  = source.last_updated

WHEN NOT MATCHED BY TARGET THEN INSERT (
  transaction_id, customer_id, customer_name, country,
  amount, transaction_date, status, last_updated
) VALUES (
  source.transaction_id, source.customer_id, source.customer_name, source.country,
  source.amount, source.transaction_date, source.status, source.last_updated
)
"""


# ──────────────────────────────────────────────────────────────
# Helper: ejecutar SQL en BigQuery
# ──────────────────────────────────────────────────────────────
def execute(client: bigquery.Client, sql: str, description: str,
            location: str = "US") -> bigquery.QueryJob:
    log.info("%s ...", description)
    job = client.query(sql, location=location)
    job.result()
    affected = getattr(job, "num_dml_affected_rows", None)
    if affected is not None:
        log.info("  Filas afectadas: %d", affected or 0)
    log.info("  OK")
    return job


# ──────────────────────────────────────────────────────────────
# PASO 1+2: Python ETL Bridge  ADLS Gen2 → BigQuery
#
# Lee el Delta Lake directamente con la librería `deltalake`
# (soporta ADLS Gen2 nativamente via azure-storage-blob).
# Convierte PyArrow → pandas → carga en BigQuery.
#
# Ventaja clave: no pasa por GCS ni depende de BigQuery Omni
# para escribir — elimina el problema cross-cloud completamente.
# ──────────────────────────────────────────────────────────────
def etl_bridge_adls_to_bq(
    bq_client: bigquery.Client,
    project: str,
    dataset: str,
    adls_account: str,
    adls_container: str,
    adls_path: str,
    adls_key: str,
) -> None:
    """
    Lee la última versión del Delta Lake desde ADLS Gen2 y carga
    directamente en dw_{env}.transactions_staging (BigQuery US).
    """
    staging_table = f"{project}.{dataset}.transactions_staging"
    uri = f"az://{adls_container}/{adls_path}"

    log.info("ETL Bridge — Leyendo Delta Lake desde ADLS Gen2")
    log.info("  URI    : %s", uri)
    log.info("  Account: %s", adls_account)

    # Opciones de autenticación para deltalake → ADLS Gen2
    storage_options = {
        "account_name": adls_account,
        "account_key":  adls_key,
    }

    # deltalake resuelve el _delta_log automáticamente y devuelve
    # siempre la última versión committed — sin necesidad de polling.
    dt = DeltaTable(uri, storage_options=storage_options)
    current_version = dt.version()
    log.info("  Versión Delta actual: %d", current_version)

    arrow_table: pa.Table = dt.to_pyarrow_dataset().to_table()
    log.info("  Filas leídas: %d | Schema: %s",
             arrow_table.num_rows,
             [f.name for f in arrow_table.schema])

    # PyArrow → pandas para load_table_from_dataframe
    df = arrow_table.to_pandas()

    log.info("ETL Bridge — Cargando en BigQuery → %s", staging_table)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    load_job = bq_client.load_table_from_dataframe(
        df,
        staging_table,
        job_config=job_config,
    )
    load_job.result()

    dest = bq_client.get_table(staging_table)
    log.info(
        "  Carga completada — %d filas en %s (versión Delta: %d)",
        dest.num_rows, staging_table, current_version,
    )


# ──────────────────────────────────────────────────────────────
# PASO 3: Crear tablas maestro y destino si no existen
# ──────────────────────────────────────────────────────────────
def setup_tables(client: bigquery.Client, project: str,
                 dataset: str, env: str) -> None:
    fmt = dict(project=project, dataset=dataset, env=env)
    execute(client, CREATE_CUSTOMERS_SQL.format(**fmt),
            f"Creando {dataset}.customers (IF NOT EXISTS)")
    execute(client, INSERT_CUSTOMERS_SQL.format(**fmt),
            f"Insertando clientes de ejemplo en {dataset}.customers (si vacía)")
    execute(client, CREATE_FINAL_TABLE_SQL.format(**fmt),
            f"Creando {dataset}.final_table (IF NOT EXISTS)")


# ──────────────────────────────────────────────────────────────
# PASO 4: MERGE
# ──────────────────────────────────────────────────────────────
def run_merge(client: bigquery.Client, project: str, dataset: str) -> None:
    execute(
        client,
        MERGE_SQL.format(project=project, dataset=dataset),
        f"Ejecutando MERGE → {dataset}.final_table",
    )


# ──────────────────────────────────────────────────────────────
# PASO 5: Labels Dataplex
# ──────────────────────────────────────────────────────────────
def apply_dataplex_labels(client: bigquery.Client, project: str,
                          dataset: str, env: str) -> None:
    labels = {
        "environment":    env,
        "owner":          "data-team",
        "domain":         "commerce",
        "classification": "internal",
    }
    for table_name in ["customers", "final_table", "transactions_staging"]:
        table_ref = f"{project}.{dataset}.{table_name}"
        try:
            table = client.get_table(table_ref)
            table.labels = labels
            client.update_table(table, ["labels"])
            log.info("Labels Dataplex aplicados a %s", table_ref)
        except Exception as exc:
            log.warning("No se pudieron aplicar labels a %s: %s", table_ref, exc)


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Parte 4 — Python ETL Bridge: Delta Lake ADLS Gen2 → BigQuery + MERGE"
    )
    p.add_argument("--project",          required=True, help="ID proyecto GCP")
    p.add_argument("--dataset",          required=True, help="Dataset BigQuery, ej: dw_dev")
    p.add_argument("--env",              required=True, choices=["dev", "qa", "prod"])
    p.add_argument("--adls-account",     required=True, help="Storage account ADLS, ej: jaredpruebadelta")
    p.add_argument("--adls-container",   required=True, help="Contenedor ADLS, ej: datalake")
    p.add_argument("--adls-path",        required=True, help="Path Delta en el contenedor, ej: transactions_uniform")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # La clave ADLS se lee desde variable de entorno (nunca como arg CLI)
    adls_key = os.environ.get("ADLS_ACCESS_KEY")
    if not adls_key:
        log.error("Variable de entorno ADLS_ACCESS_KEY no definida.")
        sys.exit(1)

    bq_client = bigquery.Client(project=args.project)

    log.info("=== Parte 4: Python ETL Bridge — Delta Lake → BigQuery ===")
    log.info("Proyecto: %s | Dataset: %s | Entorno: %s",
             args.project, args.dataset, args.env)
    log.info("ADLS: %s / %s / %s",
             args.adls_account, args.adls_container, args.adls_path)

    log.info("--- Paso 4.1-4.2: ETL Bridge ADLS Gen2 → transactions_staging (US) ---")
    etl_bridge_adls_to_bq(
        bq_client      = bq_client,
        project        = args.project,
        dataset        = args.dataset,
        adls_account   = args.adls_account,
        adls_container = args.adls_container,
        adls_path      = args.adls_path,
        adls_key       = adls_key,
    )

    log.info("--- Paso 4.3: Crear tablas maestro y destino (US) ---")
    setup_tables(bq_client, args.project, args.dataset, args.env)

    log.info("--- Paso 4.4: MERGE → final_table ---")
    run_merge(bq_client, args.project, args.dataset)

    log.info("--- Paso 4.5: Labels Dataplex ---")
    apply_dataplex_labels(bq_client, args.project, args.dataset, args.env)

    log.info("=== Pipeline finalizado correctamente para entorno: %s ===", args.env)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Error fatal: %s", exc, exc_info=True)
        sys.exit(1)