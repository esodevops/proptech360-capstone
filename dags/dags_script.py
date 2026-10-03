import os
import sys
from pathlib import Path
from datetime import timedelta

import pendulum
from dotenv import load_dotenv
from airflow.sdk import DAG
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.standard.operators.python import PythonOperator

# Let Airflow find src/ when it loads this file from the dags folder.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


def extraction():
    # run_quality also ingests the five raw CSV files.
    from src.quality import run_quality
    run_quality()


def transformation():
    from src.transform import start_spark, run_transform
    spark = start_spark()
    try:
        run_transform(spark)
    finally:
        spark.stop()


def loading():
    from src.transform import start_spark
    from src.load_postgres import load_mart, verify_mart, save_query_results
    spark = start_spark()
    try:
        load_mart(spark)
        print(verify_mart(spark))
        save_query_results(spark)
    finally:
        spark.stop()


# Read the alert address from the project .env before enabling notifications.
load_dotenv(PROJECT_ROOT / ".env")
ALERT_EMAIL = os.getenv("AIRFLOW_ALERT_EMAIL")

dag_failure_notification = send_smtp_notification(
    smtp_conn_id="smtp_default",
    from_email=ALERT_EMAIL,
    to=ALERT_EMAIL,
    subject="[Airflow] DAG {{ dag.dag_id }} failed",
    html_content="""
        <p>DAG <strong>{{ dag.dag_id }}</strong> failed.</p>
        <p>Run: {{ run_id }}</p>
        <p>Failed task: {{ ti.task_id }}</p>
        <p><a href="{{ ti.log_url }}">View task logs</a></p>
    """,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="proptech360_dag",
    default_args=default_args,
    description="proptech360 batch ETL pipeline",
    start_date=pendulum.datetime(2026, 8, 16, tz="Europe/Helsinki"),
    schedule=None,  # Use None for manual execution
    catchup=False,
    tags=["proptech360", "etl"],
    max_active_runs=1,  # Runs share the same staging and output files.
    on_failure_callback=[dag_failure_notification] if ALERT_EMAIL else None,
) as dag:

    extraction_task = PythonOperator(
        task_id="extraction_layer",
        python_callable=extraction,
        do_xcom_push=False,
    )

    transformation_task = PythonOperator(
        task_id="transformation_layer",
        python_callable=transformation,
        do_xcom_push=False,
    )

    loading_task = PythonOperator(
        task_id="loading_layer",
        python_callable=loading,
        do_xcom_push=False,
    )

    extraction_task >> transformation_task >> loading_task
