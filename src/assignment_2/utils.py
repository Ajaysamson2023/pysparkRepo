from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, lit, expr, max


def spark_session():
    spark = SparkSession.builder.master("local[1]").appName("Pyspark assignment_2").getOrCreate()
    return spark


def employee_df(spark):
    employee_data = [({"firstname": "James", "middlename": "", "lastname": "Smith"}, "03011998", "M", 3000),
                     ({"firstname": "Michael", "middlename": "Rose", "lastname": ""}, "10111998", "M", 20000),
                     ({"firstname": "Robert", "middlename": "", "lastname": "Williams"}, "02012000", "M", 3000),
                     ({"firstname": "Maria", "middlename": "Anne", "lastname": "Jones"}, "03011998", "F", 11000),
                     ({"firstname": "Jen", "middlename": "Mary", "lastname": "Brown"}, "04101998", "F", 10000)
                     ]
    employee_schema = StructType([
        StructField("name", MapType(StringType(), StringType()), True),
        StructField("dob", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])
    df = spark.createDataFrame(data=employee_data, schema=employee_schema)
    return df


def select_column(df):
    select_column_df = df.select(col("name.firstname"), col("name.lastname"), col("salary"))
    return select_column_df


def add_columns(df):
    df_col = df.withColumn("Country", lit("China")).withColumn("department", lit("computer science")).withColumn("age",
                                                                                                                 lit(26))
    return df_col


def salary_column(df):
    change_value_column = df.withColumn("salary", (col("salary") * 2))
    return change_value_column


def datatype_change(df):
    dt_change = df.withColumn("dob", col("dob").cast(StringType())).withColumn("salary",
                                                                               col("salary").cast(StringType()))
    return dt_change


def new_column(df):
    new_salary_column = df.withColumn("salary_1", col("salary") * 2)
    return new_salary_column


def rename_col(df):
    rename_col_name = df.withColumn('name', expr(
        "map('firstposition', name['firstname'], 'middleposition', name['middlename'], 'lastposition', name['lastname'])"))
    return rename_col_name


def maximum_salary(df):
    max_salary_df = df.select(col("name.firstname")).filter(col("salary") == 20000)
    return max_salary_df


def column_drop(df):
    drop_column = df.drop("department").drop("age")
    return drop_column


def distinct_value(df):
    distinct_data = df.select("dob", "salary").distinct()
    return distinct_data
