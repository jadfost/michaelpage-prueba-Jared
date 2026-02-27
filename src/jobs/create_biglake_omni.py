"""
create_biglake_omni.py
──────────────────────
Crea la tabla externa transactions_federated en dw_{env}_omni (azure-eastus2)
apuntando a ADLS Gen2 vía BigQuery Omni (format=DELTA_LAKE).

RESTRICCIÓN de BigQuery Omni: los datasets en azure-eastus2 solo admiten
tablas EXTERNAS. No se pueden crear tablas nativas ni usarlos como destino
de MERGE. Para operar con los datos en GCP, el script run_merge.py
materializa la tabla hacia dw_{env} (US) antes del MERGE.

Uso — variables de entorno requeridas:
    BQ_PROJECT       — ID del proyecto GCP
    BQ_DATASET       — Dataset base, ej: dw_dev  (el script añade _omni)
    BQ_TABLE         — Nombre de la tabla, ej: transactions_federated
    BQ_CONNECTION    — ID de la conexión Omni, ej: 348306483800.azure-eastus2.adls-biglake-conn
    ADLS_URI         — URI abfss:// de la tabla Delta en ADLS Gen2
    ADLS_ACCOUNT_NAME — Nombre de la cuenta de Azure Storage

Nota IA: Estructura base generada con asistencia de Claude (Anthropic).
"""

import os
import sys
import logging
import re

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def build_azure_uri(adls_uri: str, account: str) -> str:
    """
    Convierte URI abfss:// al formato azure:// que BigQuery Omni requiere.
    abfss://container@account.dfs.core.windows.net/path
      →  azure://account.blob.core.windows.net/container/path/
    """
    m = re.match(r"abfss://([^@]+)@[^/]+/(.+)", adls_uri)
    if not m:
        raise ValueError(f"URI ADLS inválida: {adls_uri}")
    container = m.group(1)
    base_path = m.group(2).rstrip("/")
    uri = f"azure://{account}.blob.core.windows.net/{container}/{base_path}/"
    log.info("URI BigQuery Omni (azure://): %s", uri)
    return uri


def main():
    project      = os.environ["BQ_PROJECT"]
    base_dataset = os.environ["BQ_DATASET"]       # dw_dev
    omni_dataset = base_dataset + "_omni"          # dw_dev_omni
    table        = os.environ["BQ_TABLE"]          # transactions_federated
    connection   = os.environ["BQ_CONNECTION"]     # 348306483800.azure-eastus2.adls-biglake-conn
    adls_uri     = os.environ["ADLS_URI"]
    account      = os.environ["ADLS_ACCOUNT_NAME"]

    log.info("Proyecto:      %s", project)
    log.info("Dataset Omni:  %s (azure-eastus2 — solo tablas externas)", omni_dataset)
    log.info("Tabla:         %s", table)
    log.info("Conexión:      %s", connection)
    log.info("ADLS URI:      %s", adls_uri)
    log.info("NOTA: tablas nativas se crean en %s (US) via materialización", base_dataset)

    azure_uri = build_azure_uri(adls_uri, account)

    from google.cloud import bigquery
    client = bigquery.Client(project=project)

    # DROP si existe tabla nativa (BigQuery no permite CREATE OR REPLACE sobre nativa)
    try:
        existing = client.get_table(f"{project}.{omni_dataset}.{table}")
        if existing.table_type != "EXTERNAL":
            client.delete_table(f"{project}.{omni_dataset}.{table}")
            log.info("Tabla nativa preexistente eliminada: %s.%s", omni_dataset, table)
    except Exception:
        pass

    sql = (
        f"CREATE OR REPLACE EXTERNAL TABLE `{project}.{omni_dataset}.{table}`\n"
        f"WITH CONNECTION `{connection}`\n"
        f"OPTIONS (\n"
        f"  format = 'DELTA_LAKE',\n"
        f"  uris   = ['{azure_uri}']\n"
        f");"
    )

    log.info("Ejecutando DDL en azure-eastus2...")
    log.info("SQL:\n%s", sql)

    job = client.query(sql, location="azure-eastus2")
    job.result()

    log.info("Tabla externa creada: %s.%s.%s", project, omni_dataset, table)
    log.info("Siguiente paso: materializar hacia %s.%s (US) para el MERGE", project, base_dataset)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        log.error("Error: %s", exc)
        sys.exit(1)