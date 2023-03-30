import pendulum
import logging

start_date = pendulum.datetime(2023, 1, 1)
end_date = pendulum.datetime(2023, 1, 2)

logger = logging.getLogger("airflow.task")
logger.info("end test")