# Dockerfile — Cloud Run Job: refresh_biglake.py
# Actualiza la tabla externa BigLake con la versión más reciente de Delta Lake

FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias con soporte ADLS Gen2 (azure) y GCS (google)
RUN pip install --no-cache-dir \
    "deltalake[azure]==0.17.4" \
    google-cloud-bigquery==3.17.2 \
    google-cloud-storage==2.14.0 \
    azure-storage-blob==12.19.0 \
    pyarrow==14.0.2

# Copiar scripts
COPY refresh_biglake.py .
COPY create_delta_data.py .

# Variables de entorno — se sobreescriben en Cloud Run Job
ENV GCS_BUCKET=""
ENV DELTA_PATH="delta/transactions"
ENV GCP_PROJECT=""
ENV BQ_DATASET=""
ENV BQ_TABLE="transactions_federated"
ENV ADLS_ACCOUNT_NAME=""
ENV ADLS_ACCESS_KEY=""
ENV ADLS_CONTAINER="datalake"
ENV ADLS_DELTA_PATH="transactions_uniform"

# Ejecuta refresh_biglake.py como entry point del job
ENTRYPOINT ["python", "refresh_biglake.py"]
