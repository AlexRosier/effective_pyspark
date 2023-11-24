import datetime

from pyspark.sql import DataFrame
import pyspark.sql.functions as psf
from pyspark.sql.functions import udf
import holidays
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, BooleanType


def is_belgian_holiday(date: datetime.date) -> bool:
    if date is not None:
        return date in holidays.BE()
    else:
        return None

def is_weekend(date: datetime.date) -> bool:
    return date.weekday() >= 5


def label_weekend(
    frame: DataFrame, colname: str = "date", new_colname: str = "is_weekend"
) -> DataFrame:
    my_udf = udf(is_weekend, BooleanType())
    new_frame = frame.withColumn(new_colname, my_udf(psf.col(colname)))
    return new_frame


def label_holidays(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    my_udf = udf(is_belgian_holiday, BooleanType())
    new_frame = frame.withColumn(new_colname,  my_udf(psf.col(colname)))
    return new_frame


def label_holidays2(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    my_udf = udf(is_belgian_holiday, BooleanType())
    new_frame = frame.withColumn(new_colname, psf.when(psf.col(colname) != None, my_udf(psf.col(colname))).otherwise(psf.lit(None)))
    return new_frame


def label_holidays3(
    frame: DataFrame,
    colname: str = "date",
    new_colname: str = "is_belgian_holiday",
) -> DataFrame:
    my_udf = udf(is_belgian_holiday, BooleanType())
    new_frame = frame.withColumn(new_colname, psf.when(psf.col(colname) != None, my_udf(psf.col(colname))).otherwise(psf.lit(None)))
    return new_frame
