import datetime

import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - datetime.timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
# start date to be recent one and should be executed on the same day
START_DATE = now_to_the_hour
DAG_NAME = "test_empty_operator"

dag = DAG(
    DAG_NAME,
    # run every 10 minutes
    schedule="*/10 * * * *",
    default_args={"depends_on_past": True},
    start_date=START_DATE,
    catchup=False,
)

run_this_1 = EmptyOperator(task_id="run_this_1", dag=dag)
run_this_2 = EmptyOperator(task_id="run_this_2", dag=dag)
run_this_2.set_upstream(run_this_1)
run_this_3 = EmptyOperator(task_id="run_this_3", dag=dag)
run_this_3.set_upstream(run_this_2)
run_this_4 = EmptyOperator(task_id="run_this_4", dag=dag)
run_this_4.set_upstream(run_this_3)
