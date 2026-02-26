-- ================================================================
-- Parte 3: Tabla externa BigLake apuntando a Delta Lake en GCS
-- (100% gratuito – no requiere BigQuery Omni ni conexión Azure)
-- ================================================================

-- BigQuery soporta leer tablas Delta Lake directamente desde GCS
-- con format = 'DELTA'. Infiere el schema del _delta_log.
-- La tabla es creada/actualizada automáticamente por refresh_biglake.py
-- pero puedes ejecutar esta sentencia manualmente si lo necesitas.

CREATE OR REPLACE EXTERNAL TABLE `dw_dev.transactions_federated`
OPTIONS (
  format = 'DELTA',
  uris   = ['gs://raw-dev-michaelpage-prueba/delta/transactions/']
);

-- Verificar que BigQuery lee los datos Delta desde GCS
SELECT * FROM `dw_dev.transactions_federated` LIMIT 10;


-- ================================================================
-- Parte 4: Tablas de origen
-- ================================================================

-- Tabla nativa de clientes
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

-- Datos de clientes de referencia
INSERT INTO `dw_dev.customers` (customer_id, customer_name, email, country, updated_at)
VALUES
  ('CUST-A', 'Ana García',    'ana@example.com',    'Colombia',  CURRENT_TIMESTAMP()),
  ('CUST-B', 'Bob Smith',     'bob@example.com',    'USA',       CURRENT_TIMESTAMP()),
  ('CUST-C', 'Carlos Lima',   'carlos@example.com', 'Brasil',    CURRENT_TIMESTAMP()),
  ('CUST-D', 'Diana Pérez',   'diana@example.com',  'México',    CURRENT_TIMESTAMP());


-- Tabla destino del MERGE (resultado enriquecido)
CREATE TABLE IF NOT EXISTS `dw_dev.final_table` (
  transaction_id   STRING   NOT NULL,
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
-- MERGE: transactions_federated (Delta/GCS) + customers → final_table
-- ================================================================

MERGE `dw_dev.final_table` AS target
USING (
  SELECT
    t.transaction_id,
    t.customer_id,
    c.customer_name,
    c.country,
    CAST(t.amount AS NUMERIC)        AS amount,
    DATE(t.transaction_date)         AS transaction_date,
    t.status,
    CURRENT_TIMESTAMP()              AS last_updated
  FROM `dw_dev.transactions_federated` AS t
  LEFT JOIN `dw_dev.customers`          AS c
    ON t.customer_id = c.customer_id
) AS source
ON target.transaction_id = source.transaction_id

-- Actualizar existentes si cambió algo relevante
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

-- Insertar nuevos registros
WHEN NOT MATCHED BY TARGET THEN INSERT (
  transaction_id,
  customer_id,
  customer_name,
  country,
  amount,
  transaction_date,
  status,
  last_updated
) VALUES (
  source.transaction_id,
  source.customer_id,
  source.customer_name,
  source.country,
  source.amount,
  source.transaction_date,
  source.status,
  source.last_updated
);


-- ================================================================
-- Verificación post-MERGE
-- ================================================================
SELECT
  status,
  COUNT(*)          AS total_records,
  SUM(amount)       AS total_amount,
  MAX(last_updated) AS latest_update
FROM `dw_dev.final_table`
GROUP BY status
ORDER BY total_records DESC;
