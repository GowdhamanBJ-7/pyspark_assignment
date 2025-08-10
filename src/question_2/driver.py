from os import truncate

import file_path
from pyspark.sql import SparkSession
from utils import get_masked_digit,masked_card
spark = SparkSession.builder.appName("credit_card").getOrCreate()

data = [
     ("1234567891234567",),
     ("5678912345671234",),
     ("9123456712345678",),
     ("1234567812341122",),
     ("1234567812341342",)
]

##------
# # 1.Create a Dataframe as credit_card_df with different read methods
credit_card_df = spark.createDataFrame(data,["card_number"])
#
# credit_card_df.write.mode("overwrite").option("header", True).csv("/Volumes/catalog_source/credit_card_schema/second")
#
# df = spark.read.format("csv").option(key="header", value=True).load("/Volumes/catalog_source/credit_card_schema/second")
# # 2. print number of partitions
# original_partition = df.rdd.getNumPartitions()
#
# # 3. Increase the partition size to 5
# increase_partition = df.repartition(5)
# print(increase_partition)
# increase_partition.show()
#
# # 4. Decrease the partition size back to its original partition size
# reduce_to_original = increase_partition.coalesce(original_partition)
# print(reduce_to_original)
# reduce_to_original.show()
##---


# 5.Create a UDF to print only the last 4 digits marking the remaining digits as *
# Eg: ************4567
get_masked_digit(credit_card_df).show(truncate=False)

# 6.output should have 2 columns as card_number, masked_card_number(with output of question 2)
masked_card(credit_card_df).show(truncate=False)