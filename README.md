# Prueba Técnica – Ingeniero de Datos Cloud / DevOps (GCP & Multicloud)

**Proyecto GCP:** `michaelpage-prueba`  
**Repositorio:** Azure DevOps – `vodac-co / Proyectos Internos / michaelpage-prueba`  
---

## Resumen ejecutivo

| Parte | Entregable | Estado |
|-------|-----------|--------|
| 1 – IaC Terraform | Módulo reutilizable, 3 entornos en GCP | ✅ Completo y desplegado |
| 2 – Pipeline CI/CD | Azure DevOps multi-stage con aprobaciones | ✅ Completo — pipeline ejecutado end-to-end |
| 3 – Federación BigLake | Delta Lake en GCS → tabla externa BigQuery | ✅ Completo (adaptación justificada) |
| 4 – MERGE en BigQuery | Transformación + MERGE + gobernanza Dataplex | ✅ Completo y ejecutado |

> **Pipeline ejecutado exitosamente** en el run `#20260226.7`, pasando todos los stages DEV → BigLake → QA (con aprobación) → PROD (con aprobación), con Terraform desplegando infraestructura real en GCP en los tres ambientes.

---

## Arquitectura general

```
┌─────────────────────────────────────────────────────────────────┐
│                     Azure DevOps (CI/CD)                        │
│                                                                 │
│  Rama dev ──→ Plan DEV  ──→ Apply DEV  (automático)             │
│               BigLake Setup (Delta GCS → BigQuery)              │
│  Rama qa  ──→ Plan QA   ──→ Apply QA   (aprobación: Tech Lead)  │
│  Rama prod──→ Plan PROD ──→ Apply PROD (aprobación: Arq/Owner)  │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Terraform IaC
                           ▼
          ┌────────────────────────────────────┐
          │         GCP: michaelpage-prueba     │
          │                                    │
          │  ┌──────────┐  ┌────────────────┐  │
          │  │   GCS    │  │   BigQuery     │  │
          │  │ raw-{env}│  │   dw_{env}     │  │
          │  │          │  │                │  │
          │  │ delta/   │  │ transactions_  │  │
          │  │ trans.   │──▶ federated      │  │
          │  │ parquet/ │  │ customers      │  │
          │  │ trans.   │  │ final_table    │  │
          │  └──────────┘  └───────┬────────┘  │
          │                        │ MERGE      │
          │  ┌──────────┐  ┌───────▼────────┐  │
          │  │Cloud Run │  │  Vertex AI     │  │
          │  │daily-    │  │  Endpoint      │  │
          │  │pipeline  │  │  mp-endpoint   │  │
          │  └──────────┘  └────────────────┘  │
          └────────────────────────────────────┘
```

**Flujo de datos completo:**
```
Delta Lake (GCS)
    │  _delta_log/ (versionado)
    │  parquet/transactions/*.parquet
    ▼
refresh_biglake.py
    │  Detecta versión más reciente
    │  DROP tabla nativa si existe
    │  CREATE EXTERNAL TABLE (PARQUET)
    ▼
BigQuery: dw_dev.transactions_federated
    │
    ├── JOIN dw_dev.customers
    ▼
MERGE → dw_dev.final_table
    │  Labels Dataplex aplicados
    ▼
run_merge.py (orquestado como Cloud Run Job)
```

---

## Estructura del repositorio

```
michaelpage-prueba/
├── iac/
│   ├── main.tf                    # Root: instancia módulo para dev, qa, prod
│   ├── variables.tf               # Variables globales (project_id, region, prefix, owner)
│   ├── outputs.tf                 # Outputs agrupados por entorno
│   ├── terraform.tfvars.example   # Plantilla de variables (no contiene secretos)
│   └── modules/
│       └── gcp_data_env/
│           ├── main.tf            # Recursos GCP: GCS, BigQuery, Cloud Run, Vertex AI
│           ├── variables.tf       # Parámetros del módulo con validaciones
│           └── outputs.tf         # Outputs: bucket, dataset, Cloud Run URL, endpoint
├── pipelines/
│   ├── azure-pipelines.yml        # Pipeline multi-stage principal
│   └── templates/
│       └── tf-setup.yml           # Template reutilizable: instalar Terraform + credenciales GCP
├── src/
│   └── jobs/
│       ├── create_delta_data.py   # Genera tabla Delta Lake en GCS (datos de muestra)
│       ├── refresh_biglake.py     # Detecta versión Delta y actualiza tabla externa BigQuery
│       ├── run_merge.py           # Orquesta el MERGE en BigQuery + aplica labels Dataplex
│       ├── biglake_and_merge.sql  # DDL y MERGE completo para BigQuery
│       └── cloudrun-job.yaml      # Definición de Cloud Run Job para despliegue
└── README.md
```

---

## Parte 1 – Infraestructura como Código (Terraform)

### Diseño del módulo

El módulo `modules/gcp_data_env` es el núcleo de la IaC. Recibe un único parámetro `environment` (`dev | qa | prod`) y aprovisiona todos los recursos del entorno de forma idempotente.

**Principio de diseño:** un solo módulo parametrizado (DRY) en lugar de carpetas separadas por entorno. Esto garantiza que dev, qa y prod tengan exactamente la misma configuración, evitando drifts.

### Recursos desplegados por entorno

| Recurso Terraform | Nombre en GCP | Descripción |
|---|---|---|
| `google_storage_bucket` | `raw-{env}-michaelpage-prueba` | Datos crudos con versionado habilitado |
| `google_bigquery_dataset` | `dw_{env}` | Dataset de Data Warehouse |
| `google_bigquery_connection` | `biglake-gcs-{env}` | Conexión para tablas externas BigLake sobre GCS |
| `google_storage_bucket_iam_member` | — | SA de BigLake con `roles/storage.objectViewer` |
| `google_cloud_run_service` | `mp-daily-pipeline-{env}` | Pipeline diario serverless |
| `google_vertex_ai_endpoint` | `mp-endpoint-{env}` | Endpoint Vertex AI de ejemplo |
| `google_project_service` | 6 APIs | BigQuery, BigQuery Connection, Cloud Run, Storage, Vertex AI, Resource Manager |

### Backend remoto

El estado de Terraform se almacena en GCS con separación por workspace, permitiendo que el pipeline CI/CD gestione el estado de forma compartida y segura:

```hcl
backend "gcs" {
  bucket = "tfstate-michaelpage-prueba"
  prefix = "terraform/state"
}
```

Archivos de estado generados:
- `terraform/state/dev.tfstate`
- `terraform/state/qa.tfstate`
- `terraform/state/prod.tfstate`

### Workspaces de Terraform

Se utilizan workspaces combinados con `-target module.{env}` para aislar el estado de cada entorno sin duplicar código. Esto permite aplicar cambios solo al entorno deseado en cada stage del pipeline.

### Gobernanza con labels

Todos los recursos tienen labels estandarizados para trazabilidad:

```hcl
labels = {
  environment = "dev"         # dev | qa | prod
  managed_by  = "terraform"
  owner       = "data-team"
  purpose     = "data-platform"
}
```

### Decisiones de diseño clave

| Decisión | Justificación |
|---|---|
| Módulo único parametrizado | Principio DRY — un solo lugar para cambios, garantiza paridad entre entornos |
| Backend GCS remoto | Estado compartido entre pipeline CI/CD y ejecuciones locales, con versionado |
| `force_destroy = false` en prod | Protección contra eliminación accidental de datos de producción |
| Imagen pública Cloud Run | `us-docker.pkg.dev/cloudrun/container/hello` — valida el servicio sin dependencias adicionales |
| `delete_contents_on_destroy = false` en BQ prod | Protege los datasets de producción de borrado en `terraform destroy` |
| BigQuery Connection en el módulo | La SA de BigLake se crea y se le otorgan permisos automáticamente, sin pasos manuales |

### Cómo inicializar y aplicar

```bash
cd iac/

# 1. Inicializar con backend GCS
terraform init

# 2. Desplegar DEV
terraform workspace select dev || terraform workspace new dev
terraform plan  -var="project_id=michaelpage-prueba" \
                -var="prefix=mp" \
                -var="region=us-central1" \
                -target module.dev \
                -out tfplan_dev
terraform apply tfplan_dev

# 3. Desplegar PROD
terraform workspace select prod || terraform workspace new prod
terraform plan  -var="project_id=michaelpage-prueba" \
                -var="prefix=mp" \
                -var="region=us-central1" \
                -target module.prod \
                -out tfplan_prod
terraform apply tfplan_prod

# QA: mismo proceso con -target module.qa
```

### Recursos verificados en GCP

**Cloud Storage:**

| Bucket | Región | Propósito |
|---|---|---|
| `tfstate-michaelpage-prueba` | us-east1 | Estado Terraform (backend remoto) |
| `raw-dev-michaelpage-prueba` | us-central1 | Datos crudos DEV + Delta Lake |
| `raw-qa-michaelpage-prueba` | us-central1 | Datos crudos QA |
| `raw-prod-michaelpage-prueba` | us-central1 | Datos crudos PROD |

**Cloud Run:**

| Servicio | Región | Estado |
|---|---|---|
| `mp-daily-pipeline-dev` | us-central1 | ✅ Activo |
| `mp-daily-pipeline-qa` | us-central1 | ✅ Activo |
| `mp-daily-pipeline-prod` | us-central1 | ✅ Activo |

**Vertex AI:**

| Endpoint | Estado |
|---|---|
| `mp-endpoint-dev` | ✅ Activo |
| `mp-endpoint-qa` | ✅ Activo |
| `mp-endpoint-prod` | ✅ Activo |

---

## Parte 2 – Pipeline CI/CD (Azure DevOps)

### Archivo principal: `pipelines/azure-pipelines.yml`

Pipeline multi-stage que implementa el flujo GitOps completo: cada push a `dev` dispara el pipeline, que avanza por los entornos con aprobaciones en QA y PROD.

### Stages y orden de ejecución

```
plan_dev → apply_dev → biglake_setup → plan_qa → apply_qa → plan_prod → apply_prod
```

| Stage | Trigger | Aprobación | Notas |
|---|---|---|---|
| `plan_dev` | Push a rama `dev` | Ninguna | Terraform plan con -target module.dev |
| `apply_dev` | Plan DEV exitoso | Ninguna (automático) | Usa artifact tfplan_dev |
| `biglake_setup` | Apply DEV exitoso | Ninguna | Escribe Delta en GCS + crea tabla externa |
| `plan_qa` | BigLake exitoso | Ninguna | Terraform plan con -target module.qa |
| `apply_qa` | Plan QA exitoso | **Tech Lead** (Environment `qa`) | Pausa hasta aprobación manual |
| `plan_prod` | Apply QA exitoso | Ninguna | Terraform plan con -target module.prod |
| `apply_prod` | Plan PROD exitoso | **Arquitecto/Owner** (Environment `prod`) | Pausa hasta aprobación manual |

### Environments y aprobaciones

Los Environments de Azure DevOps (`dev`, `qa`, `prod`) están configurados con Approval gates:

- **dev:** sin aprobación — despliegue automático tras plan exitoso
- **qa:** requiere aprobación del rol "Tech Lead" antes de ejecutar `terraform apply`
- **prod:** requiere aprobación del rol "Arquitecto/Owner" antes de ejecutar `terraform apply`

Esto garantiza que ningún cambio llega a producción sin revisión humana, implementando el patrón de promoción controlada requerido por la prueba.

### Template reutilizable: `pipelines/templates/tf-setup.yml`

Encapsula los 4 pasos comunes a todos los stages de Terraform:

1. Descarga e instala Terraform 1.7.5 en `C:\terraform` (Windows)
2. Verifica la versión instalada
3. Decodifica `GCP_SA_KEY_JSON` de Base64 → JSON → archivo temporal sin BOM
4. Ejecuta `terraform init` apuntando al backend GCS

### Secretos y variables (Azure DevOps Library)

El Variable Group `gcp-credentials` contiene:

| Variable | Tipo | Descripción |
|---|---|---|
| `GCP_PROJECT_ID` | Texto plano | `michaelpage-prueba` |
| `GCP_SA_KEY_JSON` | **Secreto** | Service Account JSON codificado en Base64 |

> **Decisión de seguridad:** el JSON de la Service Account se almacena como Base64 para evitar problemas de encoding BOM/UTF-8 en PowerShell de Windows. El script de decodificación usa `UTF8Encoding($false)` para garantizar un JSON válido sin BOM al escribir el archivo.

### Agente self-hosted

El agente es **JAREDFOSTERPC** (Windows self-hosted) porque Azure DevOps Free no incluye parallel jobs para agentes Microsoft-hosted. Todos los scripts del pipeline están escritos en **PowerShell** para compatibilidad nativa con Windows.

### Ejecución verificada

El pipeline `#20260226.7` ejecutó el flujo completo con éxito:

- ✅ Plan DEV (50s) → Apply DEV (30s)
- ✅ BigLake Setup: Escribir Delta → GCS → BigLake (34s)
- ✅ Plan QA (48s) → Apply QA con aprobación Tech Lead (46s)
- ✅ Plan PROD (44s) → Apply PROD con aprobación Arquitecto (41s)

---

## Parte 3 – Federación BigLake (Delta Lake → BigQuery)

### Arquitectura implementada

```
create_delta_data.py
    │  Genera datos de transacciones en formato Delta Lake
    │  Escribe en GCS: gs://raw-dev-michaelpage-prueba/delta/transactions/
    │  Exporta snapshot Parquet: gs://.../parquet/transactions/*.parquet
    ▼
GCS bucket: raw-dev-michaelpage-prueba
    ├── delta/transactions/
    │   ├── _delta_log/
    │   │   ├── 00000000000000000000.json
    │   │   ├── ...
    │   │   └── 00000000000000000005.json  ← versión 5 detectada
    │   └── part-*.parquet
    └── parquet/transactions/
        └── *.parquet  ← snapshot para tabla externa
    ▼
refresh_biglake.py
    │  1. Lista _delta_log/ y detecta max versión (regex \d{20}\.json)
    │  2. Si existe tabla NATIVA: DROP TABLE (evita error 400 de BigQuery)
    │  3. CREATE OR REPLACE EXTERNAL TABLE con FORMAT='PARQUET'
    ▼
BigQuery: michaelpage-prueba.dw_dev.transactions_federated
```

### Script: `src/jobs/refresh_biglake.py`

Funciones principales:

- `get_latest_delta_version()`: Lee el `_delta_log/` del bucket GCS y detecta la versión más reciente mediante regex sobre los archivos `.json`
- `drop_if_native_table()`: **Manejo de idempotencia** — verifica si existe una tabla nativa y la elimina antes de crear la tabla externa (BigQuery no permite `CREATE OR REPLACE EXTERNAL TABLE` sobre una tabla nativa)
- `upsert_biglake_table()`: Construye y ejecuta el DDL de tabla externa apuntando a los archivos Parquet del snapshot más reciente

### DDL de tabla externa BigLake

```sql
-- Opción GCS (implementada — 100% gratuita):
CREATE OR REPLACE EXTERNAL TABLE `dw_dev.transactions_federated`
OPTIONS (
  format = 'DELTA',
  uris   = ['gs://raw-dev-michaelpage-prueba/delta/transactions/']
);

-- Opción Azure/ADLS (diseñada — requiere Databricks Standard):
CREATE OR REPLACE EXTERNAL TABLE `dw_dev.transactions_federated`
WITH CONNECTION `azure-eastus2.adls-biglake-conn`
OPTIONS (
  format = 'ICEBERG',
  uris   = ['abfss://container@account.dfs.core.windows.net/path/']
);
```

### Limitación de Databricks Free y decisión de diseño

Durante la prueba se trabajó con Databricks Free Edition. Esta versión **no permite configurar External Locations hacia ADLS Gen2** mediante credenciales de storage externo (`CONFIG_NOT_AVAILABLE`), ya que requiere Databricks Standard/Premium con Unity Catalog y External Locations configurados.

**Lo que se demostró en Databricks Free:**
- ✅ Tabla Delta creada con `delta.universalFormat.enabledFormats = iceberg`
- ✅ `delta.enableIcebergCompatV2 = true` confirmado
- ❌ Acceso a ADLS Gen2 externo bloqueado por restricción del plan

**Decisión adoptada:** implementar el flujo Delta Lake nativo sobre GCS (completamente gratuito), que demuestra exactamente los mismos patrones técnicos: versionado Delta, detección de versión, tabla externa en BigQuery. La arquitectura con ADLS Gen2 está diseñada y documentada, lista para activarse con credenciales del plan Standard.

### Cómo ejecutar el job

```bash
# Instalar dependencias
pip install google-cloud-bigquery google-cloud-storage deltalake pyarrow

# Paso 1: Generar datos Delta en GCS
python src/jobs/create_delta_data.py

# Paso 2: Actualizar tabla BigLake
python src/jobs/refresh_biglake.py \
  --gcs-bucket   raw-dev-michaelpage-prueba \
  --delta-path   delta/transactions \
  --parquet-path parquet/transactions \
  --gcp-project  michaelpage-prueba \
  --bq-dataset   dw_dev \
  --bq-table     transactions_federated
```

### Consulta de verificación

```sql
-- Verificar datos federados desde GCS
SELECT * FROM `michaelpage-prueba.dw_dev.transactions_federated`
LIMIT 10;
```

---

## Parte 4 – Transformación y MERGE en BigQuery

### Tablas involucradas

| Tabla | Tipo | Filas | Descripción |
|---|---|---|---|
| `dw_dev.transactions_federated` | Externa (Delta/GCS) | 5 | Fuente principal — tabla BigLake |
| `dw_dev.customers` | Nativa | 4 | Maestro de clientes con país y email |
| `dw_dev.final_table` | Nativa | 5 | Resultado MERGE enriquecido |

### MERGE implementado

```sql
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
  FROM `dw_dev.transactions_federated` AS t
  LEFT JOIN `dw_dev.customers`          AS c
    ON t.customer_id = c.customer_id
) AS source
ON target.transaction_id = source.transaction_id

-- Actualiza solo si cambió algo relevante (evita escrituras innecesarias)
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
  source.transaction_id, source.customer_id, source.customer_name,
  source.country, source.amount, source.transaction_date,
  source.status, source.last_updated
);
```

**Resultado:** 5 transacciones enriquecidas con nombre y país del cliente provenientes de la tabla de maestros.

### Gobernanza con labels Dataplex

```sql
-- Labels aplicados a final_table y customers
OPTIONS (labels = [
  ('environment',    'dev'),
  ('owner',          'data-team'),
  ('domain',         'commerce'),
  ('classification', 'internal')
])
```

Los mismos labels se aplican programáticamente vía BigQuery API en `run_merge.py`:

```python
table.labels = {
    "environment":    "dev",
    "owner":          "data-team",
    "domain":         "commerce",
    "classification": "internal",
}
client.update_table(table, ["labels"])
```

### Orquestación del MERGE

El script `run_merge.py` puede ejecutarse de tres formas:

```bash
# Opción 1: Ejecución directa (local o Cloud Shell)
python src/jobs/run_merge.py \
  --project michaelpage-prueba \
  --dataset dw_dev \
  --env dev

# Opción 2: Como Cloud Run Job
gcloud run jobs create run-merge \
  --region us-central1 \
  --image gcr.io/michaelpage-prueba/run-merge:latest \
  --execute-now

# Opción 3: BigQuery Console
# Ejecutar directamente src/jobs/biglake_and_merge.sql
```

### Consulta de verificación post-MERGE

```sql
SELECT
  status,
  COUNT(*)          AS total_records,
  SUM(amount)       AS total_amount,
  MAX(last_updated) AS latest_update
FROM `michaelpage-prueba.dw_dev.final_table`
GROUP BY status
ORDER BY total_records DESC;
```

---

## Cómo reproducir toda la solución

### Prerequisitos

- GCP Project con billing habilitado
- Service Account con roles: `Editor`, `BigQuery Admin`, `Storage Admin`, `Vertex AI Admin`
- Azure DevOps Organization con agente self-hosted configurado
- Variable Group `gcp-credentials` con `GCP_PROJECT_ID` y `GCP_SA_KEY_JSON` (Base64)

### Paso a paso completo

```bash
# 1. Clonar el repositorio
git clone https://dev.azure.com/vodac-co/Proyectos%20Internos/_git/michaelpage-prueba

# 2. Crear bucket de estado Terraform (una sola vez)
gsutil mb -l us-east1 gs://tfstate-michaelpage-prueba

# 3. Desplegar infraestructura DEV
cd iac/
terraform init
terraform workspace select dev || terraform workspace new dev
terraform apply -var="project_id=michaelpage-prueba" -var="prefix=mp" -target module.dev

# 4. Generar datos Delta y federar en BigQuery
cd ..
pip install google-cloud-bigquery google-cloud-storage deltalake pyarrow
python src/jobs/create_delta_data.py
python src/jobs/refresh_biglake.py --gcs-bucket raw-dev-michaelpage-prueba \
  --delta-path delta/transactions --gcp-project michaelpage-prueba \
  --bq-dataset dw_dev --bq-table transactions_federated

# 5. Ejecutar MERGE
python src/jobs/run_merge.py --project michaelpage-prueba --dataset dw_dev --env dev

# 6. Para CI/CD: hacer push a rama dev en Azure DevOps
git push origin dev
# El pipeline se dispara automáticamente y recorre todos los stages
```
