from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
import os

# Add src folders to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/ingestion')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/processing')))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src/transformation')))

from market_ingest import read_sample_data, save_raw_data
from clean_data import clean_data
from generate_insights import generate_insights

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 11, 24),
    'retries': 1
}

dag = DAG(
    'market_insight_pipeline',
    default_args=default_args,
    description='ETL pipeline for global market insights',
    schedule_interval='@daily',
    catchup=False
)

# -------------------
# Task 1: Ingestion
# -------------------
def run_ingestion():
    df = read_sample_data("../../sample_data/stocks_sample.csv")
    today = datetime.today().strftime("%Y-%m-%d")
    save_raw_data(df, f"stocks_raw_{today}.csv")

ingestion_task = PythonOperator(
    task_id='ingestion',
    python_callable=run_ingestion,
    dag=dag
)

# -------------------
# Task 2: Cleaning
# -------------------
def run_cleaning():
    today = datetime.today().strftime("%Y-%m-%d")
    clean_data(f"stocks_raw_{today}.csv")

cleaning_task = PythonOperator(
    task_id='cleaning',
    python_callable=run_cleaning,
    dag=dag
)

# -------------------
# Task 3: Transformation / Insights
# -------------------
def run_transformation():
    today = datetime.today().strftime("%Y-%m-%d")
    generate_insights(f"cleaned_stocks_raw_{today}.csv")

transformation_task = PythonOperator(
    task_id='transformation',
    python_callable=run_transformation,
    dag=dag
)

# -------------------
# Define task order
# -------------------
ingestion_task >> cleaning_task >> transformation_task

