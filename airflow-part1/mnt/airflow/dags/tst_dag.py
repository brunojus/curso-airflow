from airflow import DAG
from airflow.operators.empty import EmptyOperator

from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'start_date': datetime(2023, 1, 1)
}

def process():
    return 'process'

with DAG(dag_id='tst_dag', schedule_interval='0 0 * * *', default_args=default_args, catchup=False) as dag:
    
    task_1 = EmptyOperator(task_id='task_1')

    task_2 = PythonOperator(task_id='task_2', python_callable=process)

    # Tasks dynamically generated 
    tasks = [EmptyOperator(task_id='task_{0}'.format(t)) for t in range(3, 6)]

    task_6 = EmptyOperator(task_id='task_6')

    task_1 >> task_2 >> tasks >> task_6