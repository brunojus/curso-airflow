from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator

from datetime import datetime, timedelta
import csv
import requests
import json

def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

def criar_tabela(sql):
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    records = pg_hook.get_records(sql=sql)
    return records

default_args = {
    "owner": "airflow",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("forex_data_pipeline_curso_hoje", start_date=datetime(2023,1,1),
    schedule_interval='@daily', default_args=default_args, tags=["dados", "financeiro"]) as dag:

    # Verificar se a minha API encontra-se disponível
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )

    # Verificar a existência do arquivo forex_currencies.csv

    is_forex_file_available = FileSensor(
        task_id="is_forex_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        timeout=20,
    )

    download_dados = PythonOperator(
        task_id="download_dados",
        python_callable=download_rates
    )

    salvar_valores = BashOperator(
        task_id="salvar_valores",
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/forex_rates.json /forex
        """
    )


    # Criar a tabela no Hive

    criar_tabela_hive = HiveOperator(
        task_id="criar_tabela_hive",
        hive_cli_conn_id="hive_conn",
        hql="""
            CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                base STRING,
                last_update DATE,
                eur DOUBLE,
                usd DOUBLE,
                nzd DOUBLE,
                gbp DOUBLE,
                jpy DOUBLE,
                cad DOUBLE
                )
            ROW FORMAT DELIMITED
            FIELDS TERMINATED BY ','
            STORED AS TEXTFILE
        
        """
    )
    # Criar tabela no postgres

    criar_tabela_postgres = PythonOperator(
        task_id = "criar_tabela_postgres",
        python_callable =criar_tabela,
        op_kwargs={
            'sql': 'CREATE TABLE IF NOT EXISTS forex_rates (base TEXT ,last_update TEXT , eur TEXT,usd TEXT, nzd TEXT,gbp TEXT,jpy TEXT,cad TEXT);'}
    )

    spark_process = SparkSubmitOperator(
        task_id = "spark_process",
        application="/opt/airflow/dags/scripts/forex_processing.py",
        conn_id="spark_conn",
        verbose=False
    )

    salvar_postgres_dados = BashOperator(
        task_id = "salvar_postgres_dados",
        bash_command='python /opt/airflow/dags/scripts/processamento_pandas.py',
        dag=dag
    )

    enviar_email = EmailOperator(
        task_id="enviar_email",
        to="bjpraciano@gmail.com",
        subject="Curso Data Pipeline",
        html_content="<h3>Sua pipeline foi finalizada</h3>"
    )

    is_api_available >> is_forex_file_available >> download_dados >> salvar_valores
    salvar_valores >> criar_tabela_hive >> criar_tabela_postgres >> salvar_postgres_dados >> enviar_email