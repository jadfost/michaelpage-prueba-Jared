-- ================================================================
-- Parte 3 — Tabla externa BigLake en dw_dev_omni (azure-eastus2)
--
-- RESTRICCIÓN de BigQuery Omni: los datasets en azure-eastus2
-- solo admiten tablas EXTERNAS. No se pueden crear tablas nativas
-- ni usar este dataset como destino de MERGE o INSERT.
--
-- Esta tabla es creada por create_biglake_omni.py en el pipeline.
-- Se incluye aquí para referencia y reproducción manual.
-- ================================================================

CREATE OR REPLACE EXTERNAL TABLE `dw_dev_omni.transactions_federated`
WITH CONNECTION `projects/michaelpage-prueba/locations/azure-eastus2/connections/adls-biglake-conn`
OPTIONS (
  format = 'DELTA_LAKE',
  uris   = ['azure://jaredpruebadelta.blob.core.windows.net/datalake/transactions_uniform/']
);

-- Verificar lectura directa desde Azure ADLS Gen2
SELECT * FROM `michaelpage-prueba.dw_dev_omni.transactions_federated` LIMIT 10;


-- ================================================================
-- Materialización: dw_dev_omni → dw_dev.transactions_staging
--
-- BigQuery no permite JOINs ni MERGE entre datasets de regiones
-- distintas. Para usar los datos de ADLS Gen2 en el MERGE, primero
-- se exportan a GCS (desde azure-eastus2) y luego se cargan en
-- dw_dev (US). run_merge.py automatiza este proceso.
--
-- Equivalente SQL (para referencia — se ejecuta vía Python API):
-- ================================================================

-- Paso A: Export a GCS (se ejecuta con location=azure-eastus2 en Python)
-- EXPORT DATA OPTIONS(uri='gs://raw-dev-michaelpage-prueba/staging/transactions_omni_*.parquet', format='PARQUET')
-- AS SELECT * FROM `dw_dev_omni.transactions_federated`;

-- Paso B: Load GCS → tabla nativa (se ejecuta con LoadJobConfig en Python)
-- La tabla transactions_staging se crea automáticamente con autodetect=True


-- ================================================================
-- Parte 4 — Tablas nativas en dw_dev (US)
-- ================================================================

-- Tabla de staging: copia materializada desde ADLS Gen2
-- (creada automáticamente por run_merge.py via LoadJobConfig)

-- Tabla maestra de clientes con labels Dataplex
CREATE TABLE IF NOT EXISTS `dw_dev.customers` (
  customer_id   STRING  NOT NULL,
  customer_name STRING,
  email         STRING,
  country       STRING,
  updated_at    TIMESTAMP,
  PRIMARY KEY (customer_id) NOT ENFORCED
)
OPTIONS (
  labels = [
    ('environment',    'dev'),
    ('owner',          'data-team'),
    ('domain',         'commerce'),
    ('classification', 'internal')
  ]
);

INSERT INTO `dw_dev.customers` VALUES
  ('CUST-A', 'Alice Johnson',  'alice@example.com',  'Colombia', CURRENT_TIMESTAMP()),
  ('CUST-B', 'Bob Smith',      'bob@example.com',    'Mexico',   CURRENT_TIMESTAMP()),
  ('CUST-C', 'Carlos Rivera',  'carlos@example.com', 'Colombia', CURRENT_TIMESTAMP()),
  ('CUST-D', 'Diana Torres',   'diana@example.com',  'Peru',     CURRENT_TIMESTAMP());


-- Tabla destino del MERGE con labels Dataplex
CREATE TABLE IF NOT EXISTS `dw_dev.final_table` (
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
    ('environment',    'dev'),
    ('owner',          'data-team'),
    ('domain',         'commerce'),
    ('classification', 'internal')
  ]
);


-- ================================================================
-- Parte 4 — MERGE
--
-- Fuente: dw_dev.transactions_staging (materializada desde ADLS Gen2)
-- Maestro: dw_dev.customers
-- Destino: dw_dev.final_table
-- Todo en dw_dev (US) — sin restricciones cross-region
-- ================================================================

MERGE `dw_dev.final_table` AS target
USING (
  SELECT
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    c.country,
    CAST(t.amount AS NUMERIC)   AS amount,
    DATE(t.transaction_date)    AS transaction_date,
    t.status,
    CURRENT_TIMESTAMP()         AS last_updated
  FROM `dw_dev.transactions_staging` AS t
  LEFT JOIN `dw_dev.customers`        AS c
    ON t.customer_id = c.customer_id
) AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED AND (
  target.status        != source.status        OR
  target.customer_name != source.customer_name OR
  target.amount        != source.amount
) THEN UPDATE SET
  customer_name    = source.customer_name,
  country          = source.country,
  amount           = source.amount,
  status           = source.status,
  last_updated     = source.last_updated

WHEN NOT MATCHED BY TARGET THEN INSERT (
  transaction_id, customer_id, customer_name, country,
  amount, transaction_date, status, last_updated
) VALUES (
  source.transaction_id, source.customer_id, source.customer_name,
  source.country, source.amount, source.transaction_date,
  source.status, source.last_updated
);


-- Verificar resultado del MERGE
SELECT
  status,
  COUNT(*)          AS total_records,
  SUM(amount)       AS total_amount,
  MAX(last_updated) AS latest_update
FROM `dw_dev.final_table`
GROUP BY status
ORDER BY total_records DESC;