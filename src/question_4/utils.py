from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType,StructType,StructField,IntegerType,ArrayType
import re

spark = SparkSession.builder.appName("read_json").getOrCreate()

def get_schema():
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("properties", StructType([
            StructField("name", StringType(), True),
            StructField("storeSize", StringType(), True)
        ])),
        StructField("employees", ArrayType(StructType([
            StructField("empId", IntegerType(), True),
            StructField("empName", StringType(), True)
        ])))
    ])

#read dynamic file
def dynamic_file(path,file_type,option_dict=None,schema=None):
    reader = spark.read.format(file_type)

    if option_dict:
        for key, value in option_dict.items():
            reader = reader.option(key, value)

    if schema:
        return reader.schema(schema).load(path)
    else:
        return reader.load(path)

#flatten
def flatten_json(df):
    df_flat = df.select(
        "id",
        "properties.*",
        explode("employees").alias("employee")
    )

    # Unpack the exploded employee struct
    return df_flat.select(
        "id",
        "name",
        "storeSize",
        "employee.empId",
        "employee.empName"
    )

#filter id == 1001
def get_filter_record(df):
    return df.filter(df["empId"]==1001)

def camel_to_snakecase(df):
    camel_cols = df.columns
    snake_cols = [re.sub(r'(?<!^)(?=[A-Z])', '_', col).lower() for col in camel_cols]
    return df.toDF(*snake_cols)

# Add a new column named load_date with the current date
def add_current_date(df):
    return df.withColumn("load_date", current_date())

# create 3 new columns as year, month, and day from the load_date column
def add_date_year_month(df):
    return df.withColumn("year",year(col("load_date"))) \
             .withColumn("month", month(col("load_date"))) \
             .withColumn("date", dayofmonth(col("load_date")))
    # return df \
    #     .withColumn("year", year(col("load_date"))
    #     .withColumn("month", month(col("load_date")))
    #     .withCoumn("date", dayofmonth(col("load_date")))
    # )