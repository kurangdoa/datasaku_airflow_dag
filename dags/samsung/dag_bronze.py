from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
from airflow.models import Variable
import os

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
# start date to be recent one and should be executed on the same day
START_DATE = now_to_the_hour
DAG_NAME = "dag_samsung_bronze"

AWS_SECRET = Variable.get("AWS_SECRET")
AWS_KEY = Variable.get("AWS_KEY")

with DAG(
    DAG_NAME,
    # run every hour
    schedule="0 0 * * *",
    default_args={"depends_on_past": False},
    start_date=START_DATE,
    catchup=False,
):
    t1 = BashOperator(
        task_id='task_samsung_bronze'
        , bash_command="""python /opt/airflow/dags/repo/dags/samsung/scripts/transformation_bronze.py"""
    )

    t1