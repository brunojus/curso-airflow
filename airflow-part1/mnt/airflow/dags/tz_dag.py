import pendulum
from airflow import DAG
from airflow.utils import timezone
from airflow.operators.empty import EmptyOperator

from datetime import timedelta, datetime

local_tz = pendulum.timezone("America/Sao_Paulo")

default_args = {
    'start_date': datetime(2023, 3, 29, 1),
    'owner': 'Airflow'
}

with DAG(dag_id='tz_dag', schedule_interval="0 1 * * *", default_args=default_args) as dag:
    dummy_task = EmptyOperator(task_id='dummy_task')
    
    run_dates = dag.get_run_dates(start_date=dag.start_date)
    next_execution_date = run_dates[-1] if len(run_dates) != 0 else None

    print('datetime from Python is Naive: {0}'.format(timezone.is_naive(datetime(2023, 9, 19))))
    print('datetime from Airflow is Aware: {0}'.format(timezone.is_naive(timezone.datetime(2023, 9, 19)) == False))
    print('[DAG:tz_dag] timezone: {0} - start_date: {1} - schedule_interval: {2} - Last execution_date: {3} - next execution_date {4} in UTC - next execution_date {5} in local time'.format(
        dag.timezone, 
        dag.default_args['start_date'], 
        dag.schedule_interval, 
        dag.latest_execution_date, 
        next_execution_date,
        local_tz.convert(next_execution_date) if next_execution_date is not None else None
        ))
dummy_task