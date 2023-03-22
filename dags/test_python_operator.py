from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta
import pendulum

now = pendulum.now(tz="UTC")
now_to_the_hour = (now - timedelta(0, 0, 0, 0, 0, 3)).replace(minute=0, second=0, microsecond=0)
# start date to be recent one and should be executed on the same day
START_DATE = now_to_the_hour
DAG_NAME = "test_python_operator"

dag = DAG(
    DAG_NAME,
    # run every hour
    schedule="0 * * * *",
    default_args={"depends_on_past": True},
    start_date=START_DATE,
    catchup=False,
)

@task(task_id="task1")
def print_context(ds=None, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print(kwargs)
    print(ds)
    return "Whatever you return gets printed in the logs"

run_this = print_context()

run_this