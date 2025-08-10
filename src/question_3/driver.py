import file_path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from utils import create_dynamic_column,get_no_of_action,convert_timeStamp_todate

spark = SparkSession.builder.appName("time").getOrCreate()

data = [
 (1, 101, 'login', '2023-09-05 08:30:00'),
 (2, 102, 'click', '2023-09-06 12:45:00'),
 (3, 101, 'click', '2023-09-07 14:15:00'),
 (4, 103, 'login', '2023-09-08 09:00:00'),
 (5, 102, 'logout', '2023-09-09 17:30:00'),
 (6, 101, 'click', '2023-09-10 11:20:00'),
 (7, 103, 'click', '2023-09-11 10:15:00'),
 (8, 102, 'click', '2023-09-12 13:10:00')
]

schema = StructType([
    StructField("login_id",IntegerType(),False),
    StructField("user_id", IntegerType(), False),
    StructField("action", StringType(), False),
    StructField("time_stamp", StringType(), False),
])

# 1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field
df = spark.createDataFrame(data,schema)
df.show()

# 2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
new_cols = ["log_id", "user_id", "user_activity", "time_stamp"]
create_dynamic_column(df,new_cols).show()

# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
get_no_of_action(df).show()

# 4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
convert_timeStamp_todate(df).show()