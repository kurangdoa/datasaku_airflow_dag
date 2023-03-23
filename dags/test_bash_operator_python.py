from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum
import scripts.test_print_script as test_print_script

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
# start date to be recent one and should be executed on the same day
START_DATE = now_to_the_hour
DAG_NAME = "test_bash_operator_python"

with DAG(
    DAG_NAME,
    # run every hour
    schedule="0 * * * *",
    default_args={"depends_on_past": True},
    start_date=START_DATE,
    catchup=False,
):
    t1 = BashOperator(
        task_id='testairflow',
        bash_command='python /scripts/test_print_script.py'
    )

    t1