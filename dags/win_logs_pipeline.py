import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id="win_log_flow",
    default_args={
        "owner": "Team D",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

healthcheck_job = SparkSubmitOperator(
    task_id="healthcheck",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

healthcheck_job_2 = SparkSubmitOperator(
    task_id="healthcheck_stage_2",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="read_data",
    conn_id="spark-conn",
    application="jobs/python/repartition.py",
    application_args=["/opt/data/source/windows_log_sample_10gb.csv", "50", "/opt/data/bronze/windows_log"],
    dag=dag
)

python_job_silver_1 = SparkSubmitOperator(
    task_id="normalize_data_event_type",
    conn_id="spark-conn",
    application="jobs/python/normalize_data.py",
    application_args=["/opt/data/bronze/windows_log", "50", "/opt/data/silver/event_type"],
    dag=dag
)

python_job_silver_2 = SparkSubmitOperator(
    task_id="normalize_data_session",
    conn_id="spark-conn",
    application="jobs/python/normalize_data_session.py",
    application_args=["/opt/data/bronze/windows_log", "50", "/opt/data/silver/session"],
    dag=dag
)

python_job_silver_3 = SparkSubmitOperator(
    task_id="normalize_data_outliners",
    conn_id="spark-conn",
    application="jobs/python/normalize_data_outliners.py",
    application_args=["/opt/data/bronze/windows_log", "50", "/opt/data/silver/outliners"],
    dag=dag
)

python_job_gold_1 = SparkSubmitOperator(
    task_id="aggregate_event_type",
    conn_id="spark-conn",
    application="jobs/python/aggregate_event_type.py",
    application_args=["/opt/data/silver/event_type", "/opt/data/gold/event_type"],
    dag=dag
)

python_job_gold_2 = SparkSubmitOperator(
    task_id="aggregate_session",
    conn_id="spark-conn",
    application="jobs/python/aggregate_session.py",
    application_args=["/opt/data/silver/session", "/opt/data/gold/session"],
    dag=dag
)

python_job_gold_3 = SparkSubmitOperator(
    task_id="aggregate_outliners",
    conn_id="spark-conn",
    application="jobs/python/aggregate_outliners.py",
    application_args=["/opt/data/silver/outliners", "/opt/data/gold/outliners"],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

silver_tasks = [python_job_silver_1, python_job_silver_2, python_job_silver_3]
gold_tasks = [python_job_gold_1, python_job_gold_2, python_job_gold_3]

start >> healthcheck_job >> python_job >> silver_tasks >> healthcheck_job_2 >> gold_tasks >> end
