import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType
spark = SparkSession.builder.appName("customer").getOrCreate()
from utils import get_customer_bought_iphone13, get_customer_brought_all_models,get_customer_iphone13_14

data = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13")
]
columns = ["customer","product_model"]
data1 = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]
columns1 = ["product_model"]

purchase_data_df = spark.createDataFrame(data,columns)
product_data_df = spark.createDataFrame(data1,columns1)

# 2.Find the customers who have bought only iphone13
print("customer who have bought only iphone13")
get_customer_bought_iphone13(purchase_data_df).show()

# 3.Find customers who upgraded from product iphone13 to product iphone14
print("customers who upgraded from product iphone13 to iphone14")
get_customer_iphone13_14(purchase_data_df).show()

#4.Find customers who have bought all models in the new Product Data
print("customer who have bought all models")
get_customer_brought_all_models(purchase_data_df,product_data_df).show()