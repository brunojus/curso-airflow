import json
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

f = open('/opt/airflow/dags/files/forex_rate_sql.json')
data = json.load(f)


df = pd.json_normalize(data=data)
df = df.rename(columns={'rates.USD': 'usd', 'rates.NZD': 'nzd',  'rates.JPY': 'jpy',  'rates.GBP': 'gbp',  'rates.CAD': 'cad'}, inplace=False)

print(df)
postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_conn') 
df.to_sql('forex_rates', postgres_sql_upload.get_sqlalchemy_engine(), if_exists='replace', chunksize=1000)

