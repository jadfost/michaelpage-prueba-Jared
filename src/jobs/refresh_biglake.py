"""
refresh_biglake.py
──────────────────
Job que detecta la última versión de la tabla Delta en GCS,
lee el snapshot Parquet exportado y crea/actualiza la tabla
externa en BigQuery con format=PARQUET.

Uso:
    python refresh_biglake.py \
        --gcs-bucket   raw-dev-michaelpage-prueba \
        --delta-path   delta/transactions \
        --parquet-path parquet/transactions \
        --gcp-project  michaelpage-prueba \
        --bq-dataset   dw_dev \
        --bq-table     transactions_federated

Nota IA: Generado con asistencia de Claude (Anthropic).
"""

import argparse
import logging
import re
import sys

from google.cloud import bigquery, storage

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def get_latest_delta_version(gcs_client, bucket_name, delta_path) -> int:
    log_prefix = f"{delta_path.strip('/')}/_delta_log/"
    bucket = gcs_client.bucket(bucket_name)
    versions = []
    for blob in bucket.list_blobs(prefix=log_prefix):
        name = blob.name.split("/")[-1]
        match = re.match(r"^(\d{20})\.json$", name)
        if match:
            versions.append(int(match.group(1)))
    if not versions:
        raise RuntimeError(f"No se encontraron commits Delta en gs://{bucket_name}/{log_prefix}")
    latest = max(versions)
    log.info("Última versión Delta detectada: %d", latest)
    return latest


def drop_if_native_table(bq_client, project, dataset, table):
    """
    Si la tabla existe como tabla NATIVA (no externa), la elimina.
    BigQuery no permite CREATE OR REPLACE EXTERNAL TABLE sobre una tabla nativa.
    """
    from google.cloud.exceptions import NotFound
    table_ref = f"{project}.{dataset}.{table}"
    try:
        existing = bq_client.get_table(table_ref)
        if existing.table_type == "TABLE":
            log.warning(
                "La tabla %s es una tabla nativa. Eliminándola para poder crear tabla externa...",
                table_ref,
            )
            bq_client.delete_table(table_ref)
            log.info("Tabla nativa eliminada: %s", table_ref)
        else:
            log.info("Tabla existente es de tipo '%s' — se reemplazará directamente.", existing.table_type)
    except NotFound:
        log.info("La tabla %s no existe aún — se creará desde cero.", table_ref)


def upsert_biglake_table(bq_client, project, dataset, table, gcs_bucket, parquet_path, delta_version):
    table_ref = f"`{project}.{dataset}.{table}`"
    parquet_uri = f"gs://{gcs_bucket}/{parquet_path.strip('/')}/*.parquet"

    # Asegurar que no exista una tabla nativa con el mismo nombre
    drop_if_native_table(bq_client, project, dataset, table)

    sql = f"""
    CREATE OR REPLACE EXTERNAL TABLE {table_ref}
    OPTIONS (
      format = 'PARQUET',
      uris   = ['{parquet_uri}']
    )
    """

    log.info("Creando tabla externa BigQuery (PARQUET): %s.%s.%s", project, dataset, table)
    log.info("URI Parquet: %s (snapshot de versión Delta %d)", parquet_uri, delta_version)

    query_job = bq_client.query(sql)
    query_job.result()

    log.info("✅ Tabla BigLake creada/actualizada: %s.%s.%s", project, dataset, table)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--gcs-bucket",   required=True)
    p.add_argument("--delta-path",   required=True)
    p.add_argument("--parquet-path", default="parquet/transactions")
    p.add_argument("--gcp-project",  required=True)
    p.add_argument("--bq-dataset",   required=True)
    p.add_argument("--bq-table",     required=True)
    return p.parse_args()


def main():
    args = parse_args()
    gcs_client = storage.Client(project=args.gcp_project)
    bq_client  = bigquery.Client(project=args.gcp_project)

    latest_version = get_latest_delta_version(gcs_client, args.gcs_bucket, args.delta_path)

    upsert_biglake_table(
        bq_client,
        project       = args.gcp_project,
        dataset       = args.bq_dataset,
        table         = args.bq_table,
        gcs_bucket    = args.gcs_bucket,
        parquet_path  = args.parquet_path,
        delta_version = latest_version,
    )

    log.info("🎉 Federación BigLake actualizada correctamente.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Error: %s", exc)
        sys.exit(1)