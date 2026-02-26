"""
run_merge.py
────────────
Orquesta la ejecución del MERGE en BigQuery.
Puede ejecutarse directamente o como Cloud Run Job.

Uso:
    python run_merge.py --project my-gcp-project --dataset dw_dev --env dev
"""

import argparse
import logging
import sys
from pathlib import Path

from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

MERGE_SQL = """
MERGE `{project}.{dataset}.final_table` AS target
USING (
  SELECT
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    c.country,
    t.amount,
    DATE(t.transaction_date)  AS transaction_date,
    t.status,
    CURRENT_TIMESTAMP()       AS last_updated
  FROM `{project}.{dataset}.transactions_federated` AS t
  LEFT JOIN `{project}.{dataset}.customers`          AS c
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


def run_merge(project: str, dataset: str) -> bigquery.QueryJob:
    client = bigquery.Client(project=project)
    sql = MERGE_SQL.format(project=project, dataset=dataset)

    log.info("Ejecutando MERGE en %s.%s.final_table …", project, dataset)
    job = client.query(sql)
    result = job.result()  # Espera a que termine
    log.info(
        "✅ MERGE completado. Filas afectadas: %d",
        job.num_dml_affected_rows or 0,
    )
    return job


def apply_dataplex_labels(project: str, dataset: str) -> None:
    """
    Aplica labels de gobernanza sobre el dataset y la tabla final.
    En producción se usaría la API de Data Catalog / Dataplex.
    """
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.final_table"
    table = client.get_table(table_ref)
    table.labels = {
        "environment":    dataset.split("_")[-1],  # dev / prod
        "owner":          "data-team",
        "domain":         "commerce",
        "classification": "internal",
    }
    client.update_table(table, ["labels"])
    log.info("Labels de gobernanza aplicados a %s", table_ref)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ejecutar MERGE en BigQuery")
    p.add_argument("--project", required=True, help="ID del proyecto GCP")
    p.add_argument("--dataset", required=True, help="Dataset de BigQuery (ej. dw_dev)")
    p.add_argument("--env",     required=True, choices=["dev", "qa", "prod"])
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_merge(args.project, args.dataset)
    apply_dataplex_labels(args.project, args.dataset)
    log.info("Pipeline de MERGE finalizado para entorno: %s", args.env)


if __name__ == "__main__":
    main()
