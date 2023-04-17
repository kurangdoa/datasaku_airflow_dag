import math

import numpy as np
import pandas as pd
import pendulum
import os
from airflow.utils.log.logging_mixin import LoggingMixin
from datasaku import datasaku_s3
import boto3
import io
# from airflow.models import Variable

def ordinal(num: int) -> str:
    """
    Function for converting a number to its ordinal form

    Arguments:
        - num (int): Number to convert
    Returns:
        - (str): Ordinal form of number
    """
    # 11, 12, and 13 logics are needed to avoid 11st, 12nd, and 13rd outcome
    if int(str(num)[-2:]) == 11:
        ordinal = "th"
    elif int(str(num)[-2:]) == 12:
        ordinal = "th"
    elif int(str(num)[-2:]) == 13:
        ordinal = "th"
    elif int(str(num)[-1:]) == 1:
        ordinal = "st"
    elif int(str(num)[-1:]) == 2:
        ordinal = "nd"
    elif int(str(num)[-1:]) == 3:
        ordinal = "rd"
    else:
        ordinal = "th"

    return str(num) + ordinal


def transform_date(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Transform the date dataframe from scratch.

    Arguments:
        - start_date (str): The start date of the period.
        - end_date (str): The end date of the period.

    Returns:
        - (DataFrame): The transformed date dataframe in the parsed tier schema.
    """

    # create the period

    start = pendulum.parse(start_date)
    end = pendulum.parse(end_date)
    period = pendulum.period(start, end)

    # base calendar

    datetime = list(period.range("days"))
    datetime_unix = [int(x.timestamp()) for x in datetime]
    date = [x.to_date_string() for x in datetime]
    date_int = [int(x.to_date_string().replace("-", "")) for x in datetime]
    week = [x.week_of_year for x in datetime]
    month = [x.month for x in datetime]
    month_2digit_format = [x.format("MM") for x in datetime]
    quarter = [x.quarter for x in datetime]
    semester = [math.ceil(x.quarter / 2) for x in datetime]
    year = [x.year for x in datetime]
    year_2digit_format = [x.format("YY") for x in datetime]
    year_month = [x.format("YYYY-MM") for x in datetime]
    year_month_int = [int(x.format("YYYYMM")) for x in datetime]

    # start and end date

    month_start_date = [x.format("YYYY-MM-[01]") for x in datetime]
    month_end_date = [x.format("YYYY-MM-[" + str(x.days_in_month) + "]") for x in datetime]

    quarter_start_date = []
    for quarter__, year__ in zip(quarter, year):
        if quarter__ == 1:
            quarter_start_date.append(str(year__) + "-" + "01" + "-" + "01")
        elif quarter__ == 2:
            quarter_start_date.append(str(year__) + "-" + "04" + "-" + "01")
        elif quarter__ == 3:
            quarter_start_date.append(str(year__) + "-" + "07" + "-" + "01")
        elif quarter__ == 4:
            quarter_start_date.append(str(year__) + "-" + "10" + "-" + "01")

    quarter_end_date = []
    for quarter__, year__ in zip(quarter, year):
        if quarter__ == 1:
            quarter_end_date.append(str(year__) + "-" + "03" + "-" + "31")
        elif quarter__ == 2:
            quarter_end_date.append(str(year__) + "-" + "06" + "-" + "30")
        elif quarter__ == 3:
            quarter_end_date.append(str(year__) + "-" + "09" + "-" + "30")
        elif quarter__ == 4:
            quarter_end_date.append(str(year__) + "-" + "12" + "-" + "31")

    semester_start_date = []
    for semester__, year__ in zip(semester, year):
        if semester__ == 1:
            semester_start_date.append(str(year__) + "-" + "01" + "-" + "01")
        elif semester__ == 2:
            semester_start_date.append(str(year__) + "-" + "07" + "-" + "01")

    semester_end_date = []
    for semester__, year__ in zip(semester, year):
        if semester__ == 1:
            semester_end_date.append(str(year__) + "-" + "06" + "-" + "30")
        elif semester__ == 2:
            semester_end_date.append(str(year__) + "-" + "12" + "-" + "31")

    year_start_date = [str(x) + "-" + "01" + "-" + "01" for x in year]
    year_end_date = [str(x) + "-" + "12" + "-" + "31" for x in year]

    # day number and other form

    day_of_month = [x.day for x in datetime]
    day_of_month_2digit_format = [x.format("DD") for x in datetime]
    day_of_week = [7 if x.day_of_week == 0 else x.day_of_week for x in datetime]
    day_of_year = [x.day_of_year for x in datetime]
    day_of_quarter = [
        (pendulum.parse(date__) - pendulum.parse(quarter_start_date__)).days + 1
        for date__, quarter_start_date__ in zip(date, quarter_start_date)
    ]
    day_of_semester = [
        (pendulum.parse(date__) - pendulum.parse(semester_start_date__)).days + 1
        for date__, semester_start_date__ in zip(date, semester_start_date)
    ]

    day_of_month_ordinal = [x.format("Do") for x in datetime]
    day_of_week_ordinal = [ordinal(x) for x in day_of_week]
    day_of_year_ordinal = [ordinal(x) for x in day_of_year]
    day_of_quarter_ordinal = [ordinal(x) for x in day_of_quarter]
    day_of_semester_ordinal = [ordinal(x) for x in day_of_semester]

    # count days in period

    days_in_current_month = [x.days_in_month for x in datetime]
    days_in_current_quarter = [
        (pendulum.parse(quarter_end_date__) - pendulum.parse(quarter_start_date__)).days + 1
        for quarter_start_date__, quarter_end_date__ in zip(quarter_start_date, quarter_end_date)
    ]
    days_in_current_semester = [
        (pendulum.parse(semester_end_date__) - pendulum.parse(semester_start_date__)).days + 1
        for semester_start_date__, semester_end_date__ in zip(
            semester_start_date, semester_end_date
        )
    ]
    days_in_current_year = [366 if x.is_leap_year() else 365 for x in datetime]

    is_weekend = [x in (6, 7) for x in day_of_week]
    is_weekday = [x not in (6, 7) for x in day_of_week]
    is_start_of_week = [x == 1 for x in day_of_week]
    is_end_of_week = [x == 7 for x in day_of_week]
    is_start_of_month = [x == 1 for x in day_of_month]
    is_end_of_month = [
        day_of_month__ == pendulum.parse(date__).days_in_month
        for day_of_month__, date__ in zip(day_of_month, date)
    ]
    is_start_of_year = [x == 1 for x in day_of_year]
    is_end_of_year = [
        (month__ == 12) & (day_of_month__ == 31)
        for month__, day_of_month__ in zip(month, day_of_month)
    ]
    is_leap_year = [x.is_leap_year() for x in datetime]

    # naming

    day_name_long = [x.format("dddd") for x in datetime]
    day_name_short = [x.format("ddd") for x in datetime]

    month_ordinal = [x.format("Mo") for x in datetime]
    month_name_long = [x.format("MMMM") for x in datetime]
    month_name_short = [x.format("MMM") for x in datetime]

    quarter_ordinal = [x.format("Qo") for x in datetime]
    quarter_name_long = [x + " Quarter " for x in quarter_ordinal]
    quarter_name_short = ["Q" + str(x) for x in quarter]

    semester_ordinal = []
    for x in semester:
        if x == 1:
            semester_ordinal.append(str(x) + "st")
        elif x == 2:
            semester_ordinal.append(str(x) + "nd")

    semester_name_long = [x + " Semester " for x in semester_ordinal]
    semester_name_short = ["H" + str(x) for x in quarter]

    year_quarter_name = [
        str(year__) + "-" + str(quarter_name_short__)
        for quarter_name_short__, year__ in zip(quarter_name_short, year)
    ]
    year_semester_name = [
        str(year__) + "-" + str(semester_name_short__)
        for semester_name_short__, year__ in zip(semester_name_short, year)
    ]

    # previous and next dates

    prev_1d_date = [x.add(days=-1).to_date_string() for x in datetime]
    prev_7d_date = [x.add(days=-7).to_date_string() for x in datetime]
    prev_14d_date = [x.add(days=-14).to_date_string() for x in datetime]
    prev_21d_date = [x.add(days=-21).to_date_string() for x in datetime]
    prev_28d_date = [x.add(days=-28).to_date_string() for x in datetime]
    prev_30d_date = [x.add(days=-30).to_date_string() for x in datetime]
    prev_60d_date = [x.add(days=-60).to_date_string() for x in datetime]
    prev_90d_date = [x.add(days=-90).to_date_string() for x in datetime]
    prev_180d_date = [x.add(days=-180).to_date_string() for x in datetime]

    prev_month_date = [x.add(months=-1).to_date_string() for x in datetime]
    prev_quarter_date = [x.add(months=-3).to_date_string() for x in datetime]
    prev_semester_date = [x.add(months=-6).to_date_string() for x in datetime]
    prev_year_date = [x.add(years=-1).to_date_string() for x in datetime]

    next_1d_date = [x.add(days=1).to_date_string() for x in datetime]
    next_7d_date = [x.add(days=7).to_date_string() for x in datetime]
    next_14d_date = [x.add(days=14).to_date_string() for x in datetime]
    next_21d_date = [x.add(days=21).to_date_string() for x in datetime]
    next_28d_date = [x.add(days=28).to_date_string() for x in datetime]
    next_30d_date = [x.add(days=30).to_date_string() for x in datetime]
    next_60d_date = [x.add(days=60).to_date_string() for x in datetime]
    next_90d_date = [x.add(days=90).to_date_string() for x in datetime]
    next_180d_date = [x.add(days=180).to_date_string() for x in datetime]

    next_month_date = [x.add(months=1).to_date_string() for x in datetime]
    next_quarter_date = [x.add(months=3).to_date_string() for x in datetime]
    next_semester_date = [x.add(months=6).to_date_string() for x in datetime]
    next_year_date = [x.add(years=1).to_date_string() for x in datetime]

    # days passed and remaining

    days_passed_current_week = [x - 1 for x in day_of_week]
    days_passed_current_month = [x - 1 for x in day_of_month]
    days_passed_current_quarter = [
        (pendulum.parse(date__) - pendulum.parse(quarter_start_date__)).days
        for quarter_start_date__, date__ in zip(quarter_start_date, date)
    ]
    days_passed_current_semester = [
        (pendulum.parse(date__) - pendulum.parse(semester_start_date__)).days
        for semester_start_date__, date__ in zip(semester_start_date, date)
    ]
    days_passed_current_year = [x - 1 for x in day_of_year]

    days_remaining_current_week = [7 - x for x in day_of_week]
    days_remaining_current_month = [
        days_in_current_month__ - day_of_month__
        for days_in_current_month__, day_of_month__ in zip(days_in_current_month, day_of_month)
    ]
    days_remaining_current_quarter = [
        (pendulum.parse(quarter_end_date__) - pendulum.parse(date__)).days
        for quarter_end_date__, date__ in zip(quarter_end_date, date)
    ]
    days_remaining_current_semester = [
        (pendulum.parse(semester_end_date__) - pendulum.parse(date__)).days
        for semester_end_date__, date__ in zip(semester_end_date, date)
    ]
    days_remaining_current_year = [
        days_in_current_year__ - day_of_year__
        for days_in_current_year__, day_of_year__ in zip(days_in_current_year, day_of_year)
    ]

    # create dataframe

    data = {
        "date": date,
        "date_int": date_int,
        "datetime_unix": datetime_unix,
        "week": week,
        "month": month,
        "month_2digit_format": month_2digit_format,
        "month_ordinal": month_ordinal,
        "quarter": quarter,
        "quarter_ordinal": quarter_ordinal,
        "semester": semester,
        "semester_ordinal": semester_ordinal,
        "year": year,
        "year_2digit_format": year_2digit_format,
        "year_month": year_month,
        "year_month_int": year_month_int,
        "month_start_date": month_start_date,
        "month_end_date": month_end_date,
        "quarter_start_date": quarter_start_date,
        "quarter_end_date": quarter_end_date,
        "semester_start_date": semester_start_date,
        "semester_end_date": semester_end_date,
        "year_start_date": year_start_date,
        "year_end_date": year_end_date,
        "day_of_month": day_of_month,
        "day_of_month_2digit_format": day_of_month_2digit_format,
        "day_of_month_ordinal": day_of_month_ordinal,
        "day_of_week": day_of_week,
        "day_of_week_ordinal": day_of_week_ordinal,
        "day_of_quarter": day_of_quarter,
        "day_of_quarter_ordinal": day_of_quarter_ordinal,
        "day_of_semester": day_of_semester,
        "day_of_semester_ordinal": day_of_semester_ordinal,
        "day_of_year": day_of_year,
        "day_of_year_ordinal": day_of_year_ordinal,
        "days_in_current_month": days_in_current_month,
        "days_in_current_quarter": days_in_current_quarter,
        "days_in_current_semester": days_in_current_semester,
        "days_in_current_year": days_in_current_year,
        "is_weekend": is_weekend,
        "is_weekday": is_weekday,
        "is_start_of_week": is_start_of_week,
        "is_end_of_week": is_end_of_week,
        "is_start_of_month": is_start_of_month,
        "is_end_of_month": is_end_of_month,
        "is_start_of_year": is_start_of_year,
        "is_end_of_year": is_end_of_year,
        "is_leap_year": is_leap_year,
        "day_name_long": day_name_long,
        "day_name_short": day_name_short,
        "month_name_long": month_name_long,
        "month_name_short": month_name_short,
        "quarter_name_long": quarter_name_long,
        "quarter_name_short": quarter_name_short,
        "semester_name_long": semester_name_long,
        "semester_name_short": semester_name_short,
        "year_quarter_name": year_quarter_name,
        "year_semester_name": year_semester_name,
        "prev_1d_date": prev_1d_date,
        "prev_7d_date": prev_7d_date,
        "prev_14d_date": prev_14d_date,
        "prev_21d_date": prev_21d_date,
        "prev_28d_date": prev_28d_date,
        "prev_30d_date": prev_30d_date,
        "prev_60d_date": prev_60d_date,
        "prev_90d_date": prev_90d_date,
        "prev_180d_date": prev_180d_date,
        "prev_month_date": prev_month_date,
        "prev_quarter_date": prev_quarter_date,
        "prev_semester_date": prev_semester_date,
        "prev_year_date": prev_year_date,
        "next_1d_date": next_1d_date,
        "next_7d_date": next_7d_date,
        "next_14d_date": next_14d_date,
        "next_21d_date": next_21d_date,
        "next_28d_date": next_28d_date,
        "next_30d_date": next_30d_date,
        "next_60d_date": next_60d_date,
        "next_90d_date": next_90d_date,
        "next_180d_date": next_180d_date,
        "next_month_date": next_month_date,
        "next_quarter_date": next_quarter_date,
        "next_semester_date": next_semester_date,
        "next_year_date": next_year_date,
        "days_passed_current_week": days_passed_current_week,
        "days_passed_current_month": days_passed_current_month,
        "days_passed_current_quarter": days_passed_current_quarter,
        "days_passed_current_semester": days_passed_current_semester,
        "days_passed_current_year": days_passed_current_year,
        "days_remaining_current_week": days_remaining_current_week,
        "days_remaining_current_month": days_remaining_current_month,
        "days_remaining_current_quarter": days_remaining_current_quarter,
        "days_remaining_current_semester": days_remaining_current_semester,
        "days_remaining_current_year": days_remaining_current_year,
    }

    # create pandas dataframe
    df = pd.DataFrame(data)

    # convert column type
    coldate = [
        "date",
        "month_start_date",
        "quarter_start_date",
        "semester_start_date",
        "year_start_date",
        "month_end_date",
        "quarter_end_date",
        "semester_end_date",
        "year_end_date",
        "prev_1d_date",
        "prev_7d_date",
        "prev_14d_date",
        "prev_21d_date",
        "prev_28d_date",
        "prev_30d_date",
        "prev_60d_date",
        "prev_90d_date",
        "prev_180d_date",
        "prev_month_date",
        "prev_quarter_date",
        "prev_semester_date",
        "prev_year_date",
        "next_1d_date",
        "next_7d_date",
        "next_14d_date",
        "next_21d_date",
        "next_28d_date",
        "next_30d_date",
        "next_60d_date",
        "next_90d_date",
        "next_180d_date",
        "next_month_date",
        "next_quarter_date",
        "next_semester_date",
        "next_year_date",
    ]
    df.loc[:, coldate] = df.loc[:, coldate].apply(
        lambda x: pd.to_datetime(x, format="%Y-%m-%d", errors="coerce").dt.date
    )
    df.loc[:, ["datetime_unix"]] = df.loc[:, ["datetime_unix"]].astype(np.int64)

    return df

    # df = spark.createDataFrame(df)

    # return df.select(
    #     [
    #         fs.col(column__).cast("int")
    #         if (type__ == "bigint") & (column__ != "datetime_unix")
    #         else column__
    #         for column__, type__ in df.dtypes
    #     ]
    # )

start_date = os.environ.get('start_date')
end_date = os.environ.get('end_date')
aws_secret = os.environ.get('AWS_SECRET')
aws_key = os.environ.get('AWS_KEY')
LoggingMixin().log.info(aws_secret)
LoggingMixin().log.info(aws_key)
aws_secret = os.getenv("AIRFLOW_VAR_AWS_SECRET")
aws_key = os.getenv("AIRFLOW_VAR_AWS_KEY")
LoggingMixin().log.info(aws_secret)
LoggingMixin().log.info(aws_key)
# aws_secret = Variable.get("AWS_SECRET")
# aws_key = Variable.get("AWS_KEY")

df = transform_date(start_date, end_date)

LoggingMixin().log.info("start_date: %s", start_date)
LoggingMixin().log.info("end_date: %s", end_date)
LoggingMixin().log.info("variable get")
LoggingMixin().log.info(aws_secret)
LoggingMixin().log.info(aws_key)
LoggingMixin().log.info("sample dataset: ")
LoggingMixin().log.info(df.head())

boto = datasaku_s3.ConnS3(aws_access_key_id = aws_secret, aws_secret_access_key = aws_key)

# upload the file
boto.s3_upload_file('datasaku', 'self_generated_data/dim_date.parquet', df)

# class ConnS3:
#     """Class for S3"""
#     def __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token = None):
#         self.aws_id =  aws_access_key_id
#         self.aws_key = aws_secret_access_key
#         self.aws_token = aws_session_token

#         # create session
#         if (self.aws_id is not None) & (self.aws_key is not None):
#             self.s3_session = boto3.Session(
#                 aws_access_key_id = self.aws_id,
#                 aws_secret_access_key = self.aws_key,
#                 aws_session_token = self.aws_token,
#             ) # create aws session if there is aws credential defined
#         else:
#             self.s3_session = boto3.Session() # create aws session using using environment detail 

#     def s3_list_bucket(self):
#         """list bucket inside s3"""
#         s3_resource = self.s3_session.resource('s3') # create s3 resource
#         buckets = [bucket.name for bucket in s3_resource.buckets.all()] # list down the bucket
#         return buckets

#     def s3_upload_file(self, s3_bucket_def, s3_file_def, df_def):
#         """list files inside bucket s3"""
#         s3_client = self.s3_session.client('s3') # create aws client
#         with io.BytesIO() as parquet_buffer:
#             df_def.to_parquet(parquet_buffer, index=False) # expoet pandas to parquet
#             response = s3_client.put_object(
#                 Bucket = s3_bucket_def
#                 , Key = s3_file_def
#                 , Body = parquet_buffer.getvalue()
#                 ) # put object inside bucket and file path
#             status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
#             if status == 200:
#                 print(f"Successful S3 put_object response. Status - {status}") # status if success
#             else:
#                 print(f"Unsuccessful S3 put_object response. Status - {status}") # status if not success


# # conn = BaseHook.get_connection('aws_default')
# # LoggingMixin().log.info(conn.get_extra())

# boto = ConnS3(aws_access_key_id = aws_secret, aws_secret_access_key = aws_key)
# my_bucket = boto.s3_list_bucket()
# LoggingMixin().log.info(my_bucket)
# boto.s3_upload_file('datasaku', 'self_generated_data/dim_date.parquet', df)