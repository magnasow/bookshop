from airflow import DAG
from airflow.operators.python import PythonOperator  # Correct import
from datetime import datetime, timedelta  # Tu as oublié timedelta
from airflow.hooks.base import BaseHook  # Correct import
from dotenv import load_dotenv
import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))

# Charger les variables d'environnement
load_dotenv()

# Importer la fonction d'ingestion
from bookshop_dbt.ingest_postgres_to_snowflake import load_data_to_snowflake  
# Définir le DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_postgres_to_snowflake',
    default_args=default_args,
    description='Ingestion des données PostgreSQL vers Snowflake',
    schedule_interval='@daily',  # À adapter selon le besoin (ici, une fois par jour)
    start_date=datetime(2025, 4, 5),
    catchup=False,
)

# Tâche d’ingestion dans Snowflake
def ingest_data():
    load_data_to_snowflake()

ingest_task = PythonOperator(
    task_id='ingest_data',
    python_callable=ingest_data,
    dag=dag,
)

ingest_task
