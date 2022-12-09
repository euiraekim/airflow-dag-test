from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

dag = DAG(dag_id="test_dag")

t1 = DummyOperator(
        task_id="dummy_test",
        dag=dag)

t2 = SSHOperator(
        task_id='SSHOperator',
        ssh_conn_id='emr-spark',
        command='spark-submit --jars /usr/share/aws/redshift/jdbc/RedshiftJDBC.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-redshift.jar,/usr/share/aws/redshift/spark-redshift/lib/spark-avro.jar,/usr/share/aws/redshift/spark-redshift/lib/minimal-json.jar test.py')

t1 >> t2
