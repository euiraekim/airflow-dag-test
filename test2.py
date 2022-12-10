from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

import psycopg2

import redshift_connector

dag = DAG(dag_id="test2_dag", start_date = datetime(2022,12,10))

t1 = DummyOperator(
        task_id="dummy_test2",
        dag=dag)


def get_postgresql_conn():
        try:
            dbname = 'postgresql'
            host = 'postgresql.cu8vcr2xeenz.ap-northeast-2.rds.amazonaws.com'
            port = 5432
            user = 'testuser'
            pw = 'Testpw1234'
            conn = psycopg2.connect(f'dbname={dbname} host={host} port={port} user={user} password={pw}')
            conn.autocommit = True
            return conn
        except Exception as e:
            print(e)

def query():
    insert_query = "insert into sign_up_count values ('aaa');"

    select_query = """
        select * from sign_up_count;
        """

    conn = get_postgresql_conn()

    with conn.cursor() as cursor:
        try:
            cursor.execute(insert_query)
            #rows = cursor.fetchall()
            #print('rows'+ str(rows))
        except Exception as e:
            print(e)

t3 = PythonOperator(
            task_id = 'query', 
            python_callable = query,
            dag = dag
            )

t1 >> t3
