import file_path
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, array_contains
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("ass").getOrCreate()

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

# print("original dataframe")
# purchase_data_df.show()
# product_data_df.show()

# 2.Find the customers who have bought only iphone13
purchase_data_df.filter(purchase_data_df["product_model"] == "iphone13").show()

# 3.Find customers who upgraded from product iphone13 to product iphone14
agg_purchase_list = purchase_data_df.groupBy("customer").agg(collect_list("product_model").alias("product"))
a = agg_purchase_list.filter(array_contains("product","iphone13") &
                         array_contains("product","iphone14"))
a.select("customer").show()

# 4.Find customers who have bought all models in the new Product Data

