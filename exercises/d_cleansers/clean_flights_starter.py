# Exercise:
# “clean” a CSV file using PySpark.
# * Grab sample data from https://packages.revolutionanalytics.com/datasets/AirlineSubsetCsv.tar.gz
#   A copy of the data can be found on our S3 bucket (link shared in class).
# * Inspect the columns: what type of data do they hold?
# * Create an ETL job with PySpark where you read in the csv file, and perform
#   the cleansing steps mentioned in the classroom:
#   - improve column names (subjective)
#   - fix data types
#   - flag missing or unknown data
#   - remove redundant data
# * Write the data to a parquet file. How big is the parquet file compared
#   to the compressed csv file? And compared to the uncompressed csv file?
#   How long does your processing take?
# * While your job is running, open up the Spark UI and get a feel for what's
#   there (together with the instructor).
# For explanations on the columns, check https://www.transtats.bts.gov/Fields.asp?gnoyr_VQ=FGK
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as psf
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, BooleanType


def read_data(path: Path):
    spark = SparkSession.builder.getOrCreate()

    fields = [
        StructField("FL_DATE", DateType(), nullable=True),
        StructField("UNIQUE_CARRIER", StringType(), nullable=True),
        StructField("TAIL_NUM", StringType(), nullable=True),
        StructField("FL_NUM", IntegerType(), nullable=True),
        StructField("ORIGIN_AIRPORT_ID", IntegerType(), nullable=True),
        StructField("ORIGIN", StringType(), nullable=True),
        StructField("ORIGIN_STATE_ABR", StringType(), nullable=True),
        StructField("DEST_AIRPORT_ID", IntegerType(), nullable=True),
        StructField("DEST", IntegerType(), nullable=True),
        StructField("DEST_STATE_ABR", IntegerType(), nullable=True),
        StructField("CRS_DEP_TIME", StringType(), nullable=True),
        StructField("DEP_TIME", DateType(), nullable=True),
        StructField("DEP_DELAY", StringType(), nullable=True),
        StructField("DEP_DELAY_NEW", StringType(), nullable=True),
        StructField("DEP_DEL15", StringType(), nullable=True),
        StructField("DEP_DELAY_GROUP", StringType(), nullable=True),
        StructField("TAXI_OUT", StringType(), nullable=True),
        StructField("WHEELS_OFF", StringType(), nullable=True),
        StructField("WHEELS_ON", StringType(), nullable=True),
        StructField("TAXI_IN", StringType(), nullable=True),
        StructField("CRS_ARR_TIME", StringType(), nullable=True),
        StructField("ARR_TIME", StringType(), nullable=True),
        StructField("ARR_DELAY", StringType(), nullable=True),
        StructField("ARR_DELAY_NEW", IntegerType(), nullable=True),
        StructField("ARR_DEL15", StringType(), nullable=True),
        StructField("ARR_DELAY_GROUP", StringType(), nullable=True),
        StructField("CANCELLED", BooleanType(), nullable=True),
        StructField("CANCELLATION_CODE", StringType(), nullable=True),
        StructField("DIVERTED", StringType(), nullable=True),
        StructField("CRS_ELAPSED_TIME", StringType(), nullable=True),
        StructField("ACTUAL_ELAPSED_TIME", DateType(), nullable=True),
        StructField("AIR_TIME", StringType(), nullable=True),
        StructField("FLIGHTS", StringType(), nullable=True),
        StructField("DISTANCE", StringType(), nullable=True),
        StructField("DISTANCE_GROUP", StringType(), nullable=True),
        StructField("CARRIER_DELAY", StringType(), nullable=True),
        StructField("WEATHER_DELAY", StringType(), nullable=True),
        StructField("NAS_DELAY", StringType(), nullable=True),
        StructField("SECURITY_DELAY", StringType(), nullable=True),
        StructField("LATE_AIRCRAFT_DELAY", StringType(), nullable=True)
    ]

    return spark.read.csv(
        str(path),
        # For a CSV, `inferSchema=False` means every column stays of the string
        # type. There is no time wasted on inferring the schema, which is
        # arguably not something you would depend on in production either.
        schema=StructType(fields),
        header=True,
        # The dataset mixes two values for null: sometimes there's an empty attribute,
        # which you will see in the CSV file as two neighboring commas. But there are
        # also literal "null" strings, like in this sample: `420.0,null,,1.0,`
        # The following option makes a literal null-string equivalent to the empty value.
        nullValue="null",
    )


def clean(frame: DataFrame) -> DataFrame:
    df_renamed = frame.withColumnRenamed("FL_DATE", "FLIGHT_DATE").withColumnRenamed("FL_NUM", "FLIGHT_NUMBER").withColumnRenamed("TAIL_NUM", "TAIL_NUMBER")

    df_cleaned = df_renamed.withColumn("DEPARTURE_DELAY_CLEANED", psf.when(psf.col("DEP_DELAY") < 0, 0).otherwise(psf.col("DEP_DELAY")))
    df_dropped = df_cleaned.drop("YEAR","MONTH","DAY_OF_MONTH","DAY_OF_WEEK", "_c44")
    df_types = df_dropped.withColumn("TAXI_IN", psf.col("TAXI_IN").cast(IntegerType())).withColumn("DISTANCE", psf.col("DISTANCE").cast(IntegerType()))
    df2 = df_types.select(df_types.columns[:10])
    df3 = df_types.select(df_types.columns[10:20])
    df4 = df_types.select(df_types.columns[20:30])
    df5 = df_types.select(df_types.columns[30:40])
    df6 = df_types.select(df_types.columns[40:])


    df2.show(3)
    df3.show(3)
    df4.show(3)
    df5.show(3)
    df6.show(3)
    return df_types


if __name__ == "__main__":
    # use relative paths, so that the location of this project on your system
    # won't mean editing paths
    path_to_exercises = Path(__file__).parents[1]
    resources_dir = path_to_exercises / "resources"
    target_dir = path_to_exercises / "target"
    # Create the folder where the results of this script's ETL-pipeline will
    # be stored.
    target_dir.mkdir(exist_ok=True)

    # Extract
    frame = read_data(resources_dir / "flight")
    # Transform
    cleaned_frame = clean(frame)
    # Load
    cleaned_frame.write.parquet(
        path=str(target_dir / "cleaned_flights"),
        mode="overwrite",
        # Exercise: how much bigger are the files when the compression codec is set to "uncompressed"? And 'gzip'?
        compression="uncompressed",
    )
