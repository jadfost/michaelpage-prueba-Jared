# Prueba Técnica – Ingeniero de Datos Cloud / DevOps (GCP & Multicloud)

**Proyecto GCP:** `michaelpage-prueba`  
**Repositorio:** Azure DevOps – `vodac-co / Proyectos Internos / michaelpage-prueba`  

---

## Resumen Ejecutivo

| Parte | Objetivo | Entregable | Estado |
|-------|----------|-----------|--------|
| 1 – IaC Terraform | Aprovisionar 3 entornos en GCP | Módulo reutilizable + 3 entornos desplegados | ✅ Completo |
| 2 – CI/CD Azure DevOps | Pipeline multi-stage con aprobaciones | YAML multi-stage DEV → QA → PROD | ✅ Ejecutado end-to-end |
| 3 – Federación BigLake | Delta Lake ADLS Gen2 → BigQuery Omni | `dw_dev_omni.transactions_federated` (DELTA_LAKE) | ✅ Completo |
| 4 – MERGE en BigQuery | Transformación + tabla final enriquecida | Python ETL Bridge + MERGE + labels Dataplex | ✅ Completo |

> **Último run exitoso `#20260227.35`:** Plan DEV ✅ → Apply DEV ✅ → BigLake Setup ✅ (Pasos 1 + 2 + 3) → Plan QA ✅ → Apply QA pendiente de aprobación (Tech Lead).

---

## Arquitectura General

```
┌─────────────────────────────────────────────────────────────────┐
│                     Azure DevOps (CI/CD)                        │
│  Push a rama dev                                                │
│  Plan DEV → Apply DEV → BigLake Setup → Plan QA → ... → PROD   │
└─────────────────────────────────────────────────────────────────┘
                              │ Terraform IaC
                              ▼
      ┌───────────────────────────────────────────────────────┐
      │                GCP: michaelpage-prueba                 │
      │                                                       │
      │  GCS raw-{env}          dw_{env} (US)                 │
      │  ├── delta/             ├── transactions_staging ◄─┐  │
      │  ├── parquet/           ├── customers              │  │
      │  └── ...                └── final_table            │  │
      │                                                    │  │
      │                         dw_{env}_omni (azure-eastus2) │
      │                         └── transactions_federated    │
      │                              (solo tabla externa)     │
      └───────────────────────────────────────────────────────┘
                                                          ▲
                              BigQuery Omni (adls-biglake-conn)
                                                          │
      ┌───────────────────────────────────────────────────┘
      │  Azure ADLS Gen2: jaredpruebadelta
      │  datalake/transactions_uniform/
      │  (Delta Lake con UniForm/Iceberg)
      └──────────────────────────────────────────────────────
```

### Flujo de datos completo

```
[Python — create_delta_data.py]
  Escribe Delta Lake con UniForm/Iceberg
       │
       ▼
[Azure ADLS Gen2]
  datalake/transactions_uniform/
  ├── _delta_log/  (versionado Delta)
  └── part-*.parquet
       │
       │  BigQuery Omni (adls-biglake-conn, azure-eastus2)
       │  format = DELTA_LAKE  →  tabla externa para consultas
       ▼
[dw_dev_omni.transactions_federated]  ← SOLO lectura vía Omni
       │
       │  Python ETL Bridge (run_merge.py)
       │  deltalake → to_pyarrow_dataset().to_table()
       │  → load_table_from_dataframe  (directo, sin GCS intermedio)
       ▼
[dw_dev.transactions_staging]  ← tabla nativa en US (GCP)
       │
       │  LEFT JOIN
       ▼
[dw_dev.customers]             ← tabla nativa maestros
       │
       │  MERGE
       ▼
[dw_dev.final_table]           ← tabla nativa enriquecida
                                  con labels Dataplex
```

### Decisión de arquitectura: Python ETL Bridge

Durante la implementación se identificaron dos restricciones críticas de BigQuery Omni:

1. **Solo tablas externas en `azure-eastus2`**: los datasets Omni no admiten tablas nativas, INSERT, UPDATE ni MERGE como destino.
2. **`extract_table()` API no soporta tablas EXTERNAL**: el método de la SDK de BigQuery para exportar solo funciona con tablas nativas.

En lugar de un `EXPORT DATA` SQL cross-cloud (GCS ← azure-eastus2, con riesgo de permisos), se implementó un **Python ETL Bridge** que lee el Delta Lake directamente desde ADLS Gen2 con la librería `deltalake` y carga en BigQuery US sin intermediarios:

```
ADLS Gen2 (Delta Lake)
    │  deltalake.DeltaTable → to_pyarrow_dataset().to_table()
    ▼
PyArrow Table en memoria
    │  bigquery.load_table_from_dataframe (WRITE_TRUNCATE)
    ▼
dw_dev.transactions_staging (BigQuery US)
```

Ventajas:
- Sin dependencias cross-cloud ni permisos adicionales.
- Lee siempre la última versión del `_delta_log` automáticamente.
- Un solo salto: ADLS Gen2 → BigQuery (sin GCS intermedio).
- Totalmente idempotente (`WRITE_TRUNCATE`).

---

## Estructura del Repositorio

```
michaelpage-prueba/
├── iac/
│   ├── main.tf                        # Instancia el módulo para dev, qa y prod
│   ├── variables.tf                   # Variables globales
│   ├── outputs.tf                     # Outputs agrupados por entorno
│   ├── terraform.tfvars.example       # Plantilla sin secretos
│   └── modules/gcp_data_env/
│       ├── main.tf                    # Todos los recursos GCP del entorno
│       ├── variables.tf               # Parámetros del módulo
│       └── outputs.tf                 # Outputs: bucket, datasets, Cloud Run, Vertex AI
│
├── pipelines/
│   ├── azure-pipelines.yml            # Pipeline multi-stage dev/qa/prod con aprobaciones
│   └── templates/tf-setup.yml         # Template reutilizable: Terraform + credenciales GCP
│
├── src/jobs/
│   ├── create_delta_data.py           # Genera datos Delta Lake en ADLS Gen2 (UniForm/Iceberg)
│   ├── create_biglake_omni.py         # Crea dw_dev_omni.transactions_federated (Omni DELTA_LAKE)
│   ├── refresh_biglake.py             # Refresh manual de tabla externa GCS (mantenimiento)
│   ├── run_merge.py                   # ETL Bridge ADLS Gen2 → BigQuery + MERGE + labels
│   ├── biglake_and_merge.sql          # DDL completo reproducible en BigQuery Console
│   └── Dockerfile                     # Imagen Docker para Cloud Run Job
│
└── README.md
```

---

## Parte 1 – Infraestructura como Código (Terraform)

### Recursos por entorno

| Recurso | Nombre | Región | Descripción |
|---------|--------|--------|-------------|
| `google_storage_bucket` | `raw-{env}-michaelpage-prueba` | us-central1 | Datos crudos y staging |
| `google_bigquery_dataset` | `dw_{env}` | **US** | Dataset principal — tablas nativas |
| `google_bigquery_dataset` | `dw_{env}_omni` | **azure-eastus2** | Dataset Omni — solo tabla externa DELTA_LAKE |
| `google_cloud_run_service` | `mp-daily-pipeline-{env}` | us-central1 | Pipeline diario serverless |
| `google_vertex_ai_endpoint` | `mp-endpoint-{env}` | us-central1 | Endpoint Vertex AI |
| `google_artifact_registry_repository` | `mp-images-{env}` | us-central1 | Repositorio Docker |

### Decisiones de diseño Terraform

| Decisión | Justificación |
|----------|---------------|
| Módulo único (DRY) | Un solo lugar para cambios. Garantiza paridad entre dev, qa y prod. |
| `dw_{env}` en US | Tablas nativas, MERGE e INSERT solo funcionan en regiones GCP estándar. |
| `dw_{env}_omni` en azure-eastus2 | Requerido por BigQuery Omni — misma región que `adls-biglake-conn`. |
| Backend GCS remoto | Estado compartido entre pipeline CI/CD y ejecuciones locales. |
| `force_destroy = false` en prod | Protección contra eliminación accidental de datos. |
| APIs habilitadas via `for_each` | Idempotente, un solo bloque gestiona todas las APIs requeridas. |

### Cómo desplegar infraestructura

```bash
cd iac/

# Inicializar (descarga providers y configura backend GCS)
terraform init

# Desplegar DEV
terraform workspace select dev || terraform workspace new dev
terraform plan  -var="project_id=michaelpage-prueba" -var="prefix=mp" -target module.dev
terraform apply -var="project_id=michaelpage-prueba" -var="prefix=mp" -target module.dev

# Desplegar PROD
terraform workspace select prod || terraform workspace new prod
terraform plan  -var="project_id=michaelpage-prueba" -var="prefix=mp" -target module.prod
terraform apply -var="project_id=michaelpage-prueba" -var="prefix=mp" -target module.prod

# QA — solo plan (no obligatorio desplegar en la prueba)
terraform workspace select qa || terraform workspace new qa
terraform plan  -var="project_id=michaelpage-prueba" -var="prefix=mp" -target module.qa
```

---

## Parte 2 – Pipeline CI/CD (Azure DevOps)

### Flujo del pipeline

```
Push a rama dev
      │
      ▼
┌──────────────┐   ┌──────────────┐   ┌───────────────────────────────────────┐
│   plan_dev   │──▶│  apply_dev   │──▶│           biglake_setup               │
│ (automático) │   │ (automático) │   │  Paso 1: Delta Lake → ADLS Gen2       │
└──────────────┘   └──────────────┘   │  Paso 2: dw_dev_omni.transactions_    │
                                      │          federated (Omni, DELTA_LAKE)  │
                                      │  Paso 3: ETL Bridge + MERGE en         │
                                      │          dw_dev (US)                   │
                                      └──────────────┬────────────────────────┘
                                                     │
                                            ┌────────▼───────┐
                                            │    plan_qa      │
                                            └────────┬────────┘
                                                     │
                                        ┌────────────┴────────────┐
                                        │  APROBACIÓN: Tech Lead   │
                                        └────────────┬────────────┘
                                                     ▼
                                             apply_qa → plan_prod
                                                         │
                                          ┌──────────────┴──────────────┐
                                          │  APROBACIÓN: Arquitecto      │
                                          └──────────────┬──────────────┘
                                                         ▼
                                                    apply_prod
```

### Stage `biglake_setup` — detalle de los 3 pasos

| Paso | Script | Qué hace |
|------|--------|---------|
| **Paso 1** | `create_delta_data.py` | Escribe datos como Delta Lake (UniForm/Iceberg) en ADLS Gen2. También exporta snapshot Parquet a GCS como respaldo. |
| **Paso 2** | `create_biglake_omni.py` | Crea `dw_dev_omni.transactions_federated` con `format=DELTA_LAKE` apuntando a ADLS Gen2 vía `adls-biglake-conn` (azure-eastus2). |
| **Paso 3** | `run_merge.py` | **Python ETL Bridge:** lee Delta directamente desde ADLS Gen2 → carga en `transactions_staging` → crea `customers` + `final_table` → MERGE → labels Dataplex. |

### Variables de entorno requeridas (Library: `gcp-credentials`)

| Variable | Descripción |
|----------|-------------|
| `GCP_SA_KEY_JSON` | Service Account GCP en Base64 |
| `GCP_PROJECT_ID` | ID del proyecto (`michaelpage-prueba`) |
| `ADLS_ACCESS_KEY` | Clave de acceso Azure Storage (secret) |
| `ADLS_ACCOUNT_NAME` | Nombre de la cuenta (`jaredpruebadelta`) |
| `ADLS_CONTAINER` | Contenedor ADLS (`datalake`) |

---

## Parte 3 – Federación Delta Lake → BigQuery

### DDL ejecutado en BigQuery

```sql
CREATE OR REPLACE EXTERNAL TABLE `dw_dev_omni.transactions_federated`
WITH CONNECTION `projects/michaelpage-prueba/locations/azure-eastus2/connections/adls-biglake-conn`
OPTIONS (
  format = 'DELTA_LAKE',
  uris   = ['azure://jaredpruebadelta.blob.core.windows.net/datalake/transactions_uniform/']
);
```

### Scripts de federación

| Script | Descripción |
|--------|-------------|
| `create_delta_data.py` | Escribe Delta Lake con `delta.universalFormat.enabledFormats = iceberg`. Modo ADLS (principal) o GCS (fallback). |
| `create_biglake_omni.py` | Crea la tabla externa Omni con idempotencia (DROP si existe tabla nativa previa). Ejecuta DDL en `location="azure-eastus2"`. |
| `refresh_biglake.py` | Mantenimiento manual: detecta versión máxima del Delta log en GCS y recrea la tabla externa PARQUET en `dw_dev`. |

---

## Parte 4 – Python ETL Bridge + MERGE en BigQuery

### Cómo funciona `run_merge.py`

```python
# Paso 1: Leer Delta Lake directamente desde ADLS Gen2
dt = DeltaTable("az://datalake/transactions_uniform", storage_options)
arrow_table = dt.to_pyarrow_dataset().to_table()   # última versión del _delta_log

# Paso 2: Cargar directo en BigQuery US (sin pasar por GCS)
bq_client.load_table_from_dataframe(
    arrow_table.to_pandas(),
    "michaelpage-prueba.dw_dev.transactions_staging",
    job_config=LoadJobConfig(write_disposition=WRITE_TRUNCATE)
)

# Paso 3: Crear tablas maestro y destino (IF NOT EXISTS)
# Paso 4: MERGE transactions_staging + customers → final_table
# Paso 5: Labels Dataplex en las 3 tablas
```

### El MERGE

```sql
MERGE `dw_dev.final_table` AS target
USING (
  SELECT t.*, c.customer_name, c.country
  FROM dw_dev.transactions_staging AS t
  LEFT JOIN dw_dev.customers AS c ON t.customer_id = c.customer_id
) AS source
ON target.transaction_id = source.transaction_id

WHEN MATCHED AND (cambió status, nombre o monto)
  → UPDATE solo los campos modificados

WHEN NOT MATCHED BY TARGET
  → INSERT nueva fila enriquecida con datos del cliente
```

### Tablas resultantes en BigQuery

| Tabla | Dataset | Región | Tipo | Descripción |
|-------|---------|--------|------|-------------|
| `transactions_federated` | `dw_dev_omni` | azure-eastus2 | Externa (DELTA_LAKE) | Lectura directa desde ADLS Gen2 |
| `transactions_staging` | `dw_dev` | US | Nativa | Cargada via ETL Bridge desde ADLS Gen2 |
| `customers` | `dw_dev` | US | Nativa | Maestro de clientes con labels Dataplex |
| `final_table` | `dw_dev` | US | Nativa | Resultado del MERGE con labels Dataplex |

---

## Cómo Reproducir la Solución Completa

```bash
# 1. Desplegar infraestructura (ver Parte 1)
cd iac/ && terraform init && terraform apply ...

# 2. Instalar dependencias Python
pip install "deltalake[azure]==0.17.4" pyarrow google-cloud-bigquery \
            google-cloud-storage azure-storage-blob pandas-gbq

# 3. Escribir Delta Lake en ADLS Gen2
export ADLS_ACCOUNT_NAME=jaredpruebadelta
export ADLS_ACCESS_KEY=<clave>
export ADLS_CONTAINER=datalake
export ADLS_DELTA_PATH=transactions_uniform
python src/jobs/create_delta_data.py

# 4. Crear tabla externa Omni en BigQuery
export BQ_PROJECT=michaelpage-prueba
export BQ_DATASET=dw_dev
export BQ_TABLE=transactions_federated
export BQ_CONNECTION=348306483800.azure-eastus2.adls-biglake-conn
export ADLS_URI=abfss://datalake@jaredpruebadelta.dfs.core.windows.net/transactions_uniform
python src/jobs/create_biglake_omni.py

# 5. ETL Bridge + MERGE + labels
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa_key.json
export ADLS_ACCESS_KEY=<clave>
python src/jobs/run_merge.py \
  --project        michaelpage-prueba \
  --dataset        dw_dev             \
  --env            dev                \
  --adls-account   jaredpruebadelta   \
  --adls-container datalake           \
  --adls-path      transactions_uniform

# 6. Para CI/CD completo: push a rama dev
git push origin dev
```

### Queries de verificación

```sql
-- Datos federados desde ADLS Gen2 (BigQuery Omni)
SELECT * FROM `michaelpage-prueba.dw_dev_omni.transactions_federated` LIMIT 10;

-- Staging cargado via ETL Bridge
SELECT * FROM `michaelpage-prueba.dw_dev.transactions_staging` LIMIT 10;

-- Resultado del MERGE enriquecido
SELECT status, COUNT(*) AS total, SUM(amount) AS monto_total
FROM `michaelpage-prueba.dw_dev.final_table`
GROUP BY status;

-- Verificar labels Dataplex
SELECT table_name, option_name, option_value
FROM `dw_dev.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_name = 'labels';
```

---

## Nota sobre el Agente Self-hosted

El pipeline corre sobre el agente **JAREDFOSTERPC** (Windows self-hosted). Azure DevOps Free Tier no incluye parallel jobs para agentes Microsoft-hosted. Los scripts del pipeline están en PowerShell y usan Python 3.10 embeddable instalado en tiempo de ejecución en `C:\agent\_work\python310`.
