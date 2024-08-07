from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from clickhouse_driver import Client
import psycopg2
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 7, 13)
}

dag = DAG(
    dag_id='skryl_writeoff_dag',
    default_args=default_args,
    schedule_interval='5-55/15 * * * *',
    description='task 5 dag',
    catchup=False,
    max_active_runs=1
)


def main():
    with open('/opt/airflow/dags/keys/credentials.json') as json_file:
        param_connect = json.load(json_file)

    client_ch = Client(
        param_connect['clickhouse'][0]['host'],
        user=param_connect['clickhouse'][0]['user'],
        password=param_connect['clickhouse'][0]['password'],
        port=param_connect['clickhouse'][0]['port'],
        verify=False,
        compression=True
    )

    client_pg = psycopg2.connect(
        host=param_connect['postgres'][0]['host'],
        user=param_connect['postgres'][0]['user'],
        password=param_connect['postgres'][0]['password'],
        port=param_connect['postgres'][0]['port'],
        database=param_connect['postgres'][0]['database']
    )

    client_ch.execute("""INSERT INTO reports.sum_by_office_id_state 
                         select office_id
                             , state_id
                             , SUM(price_writeoff) sum_writeoff
                         from writeoffs_by_customers_qty
                         group by office_id, state_id""")

    to_import = """select office_id
                       , state_id 
                       , sum_writeoff
                   from reports.sum_by_office_id_state"""

    df = client_ch.query_dataframe(to_import)
    df_json = df.to_json(orient='records', date_format='iso')

    cursor = client_pg.cursor()
    cursor.execute(f"""CALL click_to_psg_procedures.load_sum_by_office_id_state(_src:='{df_json}')""")

    client_pg.commit()
    client_pg.close()


task1 = PythonOperator(
    task_id='Test_dag_1', python_callable=main, dag=dag)
