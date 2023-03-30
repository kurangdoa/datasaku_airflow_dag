import pendulum
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

start_date = pendulum.datetime(2023, 1, 1)
end_date = pendulum.datetime(2023, 1, 2)

logger = logging.getLogger("airflow.task")
logger.info("start from logger")
logger.info("start_date: %s", start_date)
logger.info("end_date: %s", end_date)
logger.info("end from logger")

LoggingMixin().log.info("start from LoggingMixin")
LoggingMixin().log.info("start_date: %s", start_date)
LoggingMixin().log.info("end_date: %s", end_date)
LoggingMixin().log.info("end from LoggingMixin")