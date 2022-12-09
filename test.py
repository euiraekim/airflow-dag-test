from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PythonOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

import redshift_connector

dag = DAG(dag_id="test_dag", start_date = datetime(2022,12,9))

t1 = DummyOperator(
        task_id="dummy_test",
        dag=dag)

t2 = SSHOperator(
        task_id='SSHOperator',
        ssh_conn_id='emr-spark',
        command='spark-submit --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar test.py')

def access_redshift():
    conn = redshift_connector.connect(
            host='redshift-test.cyernhele58c.ap-northeast-2.redshift.amazonaws.com:5439',
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

t1 >> t3
