from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import datetime

dag = DAG(dag_id="test_dag", start_date=datetime(2022, 12, 7))

t1 = DummyOperator(
        task_id="dummy_test",
        dag=dag)

t2 = SSHOperator(
        task_id='SSHOperator',
        ssh_conn_id='emr-spark',
        command='echo "Text from SSH Operator"')

t1 >> t2
