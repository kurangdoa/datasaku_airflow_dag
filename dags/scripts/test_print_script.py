import pendulum
import logging

start_date = pendulum.datetime(2023, 1, 1)
end_date = pendulum.datetime(2023, 1, 2)

print(start_date)
print(end_date)

logging.info("start_date: %s", start_date)
logging.info("end_date: %s", end_date)