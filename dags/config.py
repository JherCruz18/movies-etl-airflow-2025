import os
from datetime import datetime

# Paths - Usar las rutas de Docker/Airflow
ORIGIN_PATH = '/opt/airflow/data/origin/Latest 2025 movies Datasets.csv'
BRONZE_PATH = '/opt/airflow/data/bronze/bronze_data.csv'
SILVER_PATH = '/opt/airflow/data/silver/silver_data.csv'
GOLD_PATH = '/opt/airflow/data/gold/gold_data.csv'

# Logging
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
TIMESTAMP = datetime.now().isoformat()