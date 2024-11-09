import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id = "win_log_flow",
    default_args = {
        "owner": "Team D",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval = "@daily"
)

start = PythonOperator(
    task_id="start",
    python_callable = lambda: print("Jobs started"),
    dag=dag
)

healthcheck_job = SparkSubmitOperator(
    task_id="healthcheck",
    conn_id="spark-conn",
    application="jobs/python/wordcountjob.py",
    dag=dag
)

python_job = SparkSubmitOperator(
    task_id="read_data",
    conn_id="spark-conn",
    application="jobs/python/repartition.py",
    application_args=["/opt/data/source/windows_log_sample.csv", "3", "/opt/data/bronze/windows_log.csv"],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable = lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> healthcheck_job >> python_job >> end
