import pendulum
from airflow.utils.log.logging_mixin import LoggingMixin

start_date = pendulum.datetime(2023, 1, 1)
end_date = pendulum.datetime(2023, 1, 2)

LoggingMixin().log.info("start from LoggingMixin")
LoggingMixin().log.info("start_date: %s", start_date)
LoggingMixin().log.info("end_date: %s", end_date)
LoggingMixin().log.info("end from LoggingMixin")