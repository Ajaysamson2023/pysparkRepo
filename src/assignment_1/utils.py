from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, IntegerType, StringType


def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment").getOrCreate()
    return spark


def create_dataframe(spark):
    schema_products = StructType([StructField("Product Name", StringType(), nullable=True),
                                  StructField("Issue Date", StringType(), nullable=True),
                                  StructField("Price", IntegerType(), nullable=True),
                                  StructField("Brand", StringType(), nullable=True),
                                  StructField("Country", StringType(), nullable=True),
                                  StructField("ProductNumber", StringType(), nullable=True)])

    data_products = [("Washing Machine", "1648770933000", 20000, "Samsung", "India", "0001"),
                     ("Refrigerator ", "1648770999000", 35000, " LG", None, "0002"),
                     ("Air Cooler", "1648770948000", 45000, " Voltas", None, "0003")]

    df = spark.createDataFrame(data=data_products, schema=schema_products)

    return df


def time_stamp_format(df):
    time_format = df.withColumn("Issue Date", from_unixtime(col("Issue Date") / 1000, "yyyy-MM-dd'T'HH:mm:ssZZZZ"))
    return time_format


def date_type(time_format):
    datetype_format = time_format.withColumn("Date", date_format(col("Issue Date"), "yyyy-MM-dd"))
    return datetype_format


def remove_extra_space(df):
    remove_space = df.withColumn("Brand", trim(col("Brand")))
    return remove_space


def replace_values(df):
    replace_value = df.na.fill(" ", ["Country"])
    return replace_value


def transform_dataframe(spark):
    schema = StructType([
        StructField("SourceId", IntegerType(), nullable=True),
        StructField("TransactionNumber", IntegerType(), nullable=True),
        StructField("Language", StringType(), nullable=True),
        StructField("ModelNumber", IntegerType(), nullable=True),
        StructField("StartTime", StringType(), nullable=True),
        StructField("ProductNumber", StringType(), nullable=True)])
    data = [(150711, 123456, "EN", 456789, "2021-12-27T08:20:29.842+0000", "0001"),
            (150439, 234567, "UK", 345678, "2021-12-27T08:21:14.645+0000", "0002"),
            (150647, 345678, "ES", 234567, "2021-12-27T08:22:42.445+0000", "0003")]
    df_1 = spark.createDataFrame(data=data, schema=schema)
    return df_1


def add_column(df_1):
    df_time = df_1.withColumn("start_time_ms",
                              (unix_timestamp(col("StartTime"), "yyyy-MM-dd'T'HH:mm:ss.SSSZ")) * 1000)
    return df_time


def combine_df(df, transform_df):
    df_combine = df.join(transform_df, df.ProductNumber == transform_df.ProductNumber, "inner")
    return df_combine


def field_records(df_combine):
    get_value_en = df_combine.filter(df_combine.Language == "EN")
    return get_value_en
