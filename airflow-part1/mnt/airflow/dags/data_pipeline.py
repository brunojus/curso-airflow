from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner": "bruno",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@admin.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

def query_and_push(sql):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    records = pg_hook.get_records(sql=sql)
    return records

with DAG("data_pipeline", start_date=datetime(2023,1,1),
    schedule_interval="@daily", default_args=default_args) as dag:
    test="test"
    test_dag = BashOperator(
        task_id=f"t1_{test}",
        bash_command=f"echo {test}",
    )
    action = PythonOperator(
        task_id='push_result',
        python_callable=query_and_push,
        provide_context=True,
        op_kwargs={
            'sql': 'CREATE TABLE IF NOT EXISTS forex_rates (base TEXT ,last_update TEXT , eur TEXT,usd TEXT, nzd TEXT,gbp TEXT,jpy TEXT,cad TEXT);'},            
        dag=dag
    )
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='python /opt/airflow/dags/testpandas.py',
        dag=dag
    )
    # run_this = PythonOperator(
    #     task_id='postgres_task',
    #     python_callable=testpandas,
    #     dag=dag
    # )