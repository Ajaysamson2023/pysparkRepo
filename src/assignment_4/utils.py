from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number, avg, sum, min, max
from pyspark.sql.types import StringType, StructType, StructField, IntegerType
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("../../assignment_2.log"),
        logging.StreamHandler(sys.stdout)
    ])


def sparkSession():
    spark = SparkSession.builder.master("local[1]").appName("Assignment_4").getOrCreate()
    logging.info("Created spark session")
    return spark


def create_dataframe(spark):
    employee_schema = StructType([
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])
    employee_data = [("James", "Sales", 3000),
                     ("Michael", "Sales", 4600),
                     ("Robert", "Sales", 4100),
                     ("Maria", "Finance", 3000),
                     ("Raman", "Finance", 3000),
                     ("Scott", "Finance", 3300),
                     ("Jen", "Finance", 3900),
                     ("Jeff", "Marketing", 3000),
                     ("Kumar", "Marketing", 2000)
                     ]

    dataframe = spark.createDataFrame(data=employee_data, schema=employee_schema)
    logging.info("Created  dataframe with the details")
    return dataframe


def department_row(dataframe):
    first_row = Window.partitionBy("department").orderBy("salary")
    first_row_1 = dataframe.withColumn("row_number", row_number().over(first_row))
    row_1 = first_row_1.select("*").filter(col("row_number") == 1)
    logging.info("Got first row from each department")
    return row_1


def highest_salary(dataframe):
    high_salary = Window.partitionBy("department").orderBy(col("salary").desc())
    salary_highest = dataframe.withColumn("row_number", row_number().over(high_salary))
    highest_salary_1 = salary_highest.select("employee_name", "department", "salary", ).filter(col("row_number") == 1)
    logging.info("Got highest salary who earns highest salary ")
    return highest_salary_1


def employee_salary(dataframe):
    dept_group_all = Window.partitionBy("department")
    dept_group_all_1 = Window.partitionBy("department").orderBy(col("salary"))
    employee_salary_result = dataframe.withColumn("row_number", row_number().over(dept_group_all_1)) \
        .withColumn("average_salary", avg(col("salary")).over(dept_group_all)) \
        .withColumn("total_salary", sum(col("salary")).over(dept_group_all)) \
        .withColumn("min_salary", min(col("salary")).over(dept_group_all)) \
        .withColumn("max_salary", max(col("salary")).over(dept_group_all)) \
        .filter(col("row_number") == 1) \
        .select("employee_name", "department", "salary", "average_salary", "total_salary", "min_salary", "max_salary")
    logging.info("Got employee highest,lowest,maximum,minimum,average salary")
    return employee_salary_result
