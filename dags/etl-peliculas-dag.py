from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

from scripts.extract_bronze import extract_bronze
from scripts.extract_silver import extract_silver
from scripts.extract_gold import extract_gold

# Configurar logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ======================
#     DEFAULT ARGS
# ======================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ======================
#        DAG
# ======================

with DAG(
    dag_id="cem_pipeline",
    default_args=default_args,
    description="Pipeline ETL: Bronze -> Silver -> Gold para dataset de películas",
    schedule="@daily",
    catchup=False,
    tags=['etl', 'data-engineering', 'peliculas'],
) as dag:

    extract = PythonOperator(
        task_id="extract_bronze",
        python_callable=extract_bronze,
    )

    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=extract_silver,
    )

    load = PythonOperator(
        task_id="load_gold",
        python_callable=extract_gold,
    )

    extract >> transform >> load
