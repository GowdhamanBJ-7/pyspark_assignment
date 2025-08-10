
from pyspark.sql.functions import col, collect_list, array_contains,length, size, count


# print("original dataframe")
# purchase_data_df.show()
# product_data_df.show()

# 2.Find the customers who have bought only iphone13
def get_customer_bought_iphone13(purchase_df):
    return purchase_df.filter(purchase_df["product_model"] == "iphone13")

# 3.Find customers who upgraded from product iphone13 to product iphone14
def get_customer_iphone13_14(purchase_df):
    df3 = purchase_df.filter(col("product_model").isin("iphone13", "iphone14"))
    df3 = df3.groupBy("customer").agg(collect_list(col("product_model")).alias("purchased_mobile")) \
        .filter(size(col("purchased_mobile")) == 2)
    return df3

# 4.Find customers who have bought all models in the new Product Data
# method-1
def get_customer_brought_all_models(purchase_df, product_df):
    combine_all_model = product_df.agg(collect_list(col("product_model")).alias("all_products"))
    df4 = purchase_df.groupBy("customer").agg(collect_list(col("product_model")).alias("group_items"))
    return df4.join(combine_all_model, df4.group_items == combine_all_model.all_products, how='left_semi')


# method - 2
# print("product model single list")
# df_match_list = product_data_df.agg(
#     collect_list(col("product_model")).alias("all_products")
# ).collect()[0][0]
# print("Product model single list:", df_match_list)
#
# @udf(returnType=IntegerType())
# def check_product(customer,product_list):
#     print("udf called")
#     if product_list == df_match_list:
#         return customer
#     else:
#         return 0
#
# df4 = purchase_data_df.groupBy("customer").agg(collect_list(col("product_model")).alias("group_items"))
# df4.withColumn("match_found",check_product(df4["customer"],df4["group_items"])).show(truncate=False)

