from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator

from datetime import datetime
import psycopg2
import redshift_connector

dag = DAG(dag_id="order_processing",
        start_date = datetime(2022, 12, 11, 12),
        schedule_interval = '30 * * * *',)

start_task = DummyOperator(
                task_id="start",
                dag=dag)


spark_task = SSHOperator(
        task_id='spark-s3-to-redshift',
        ssh_conn_id='emr-spark',
        command='spark-submit --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar users_to_redshift.py')

def access_redshift():
    conn = redshift_connector.connect(
            host='redshift-test.cyernhele58c.ap-northeast-2.redshift.amazonaws.com',
            database='redshift_test',
            user='testuser',
            password='Testpw1234')

    cursor = conn.cursor()
    cursor.execute("select * from users")
    result = cursor.fetchall()
    print('result : ', result)


t3 = PythonOperator(
            task_id = 'access_redshift', 
            python_callable = access_redshift,
            dag = dag
            )

start_task >> spark_task
