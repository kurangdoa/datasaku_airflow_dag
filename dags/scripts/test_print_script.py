import pendulum
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

start_date = pendulum.datetime(2023, 1, 1)
end_date = pendulum.datetime(2023, 1, 2)

logger = logging.getLogger("airflow.task")
logger.info("from logger")

LoggingMixin().log.info("from LoggingMixin")