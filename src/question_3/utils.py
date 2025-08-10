from pyspark.sql.functions import collect_list,col,to_date,date_sub,current_date,count

# 2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
def create_dynamic_column(df,columns):
    return df.toDF(*columns)

# 3. Write a query to calculate the number of actions performed by each user in the last 7 days
def get_no_of_action(df):
    df = df.withColumn("time_stamp", to_date(df["time_stamp"]))
    last_7_days_df = df.filter(df["time_stamp"] >= date_sub(current_date(), 7))
    return last_7_days_df.groupBy("user_id").agg(count("*"))

# 4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
def convert_timeStamp_todate(df):
    df = df.withColumn("time_stamp", to_date(df["time_stamp"]))
    return df.withColumn("login_date", to_date(col("time_stamp"), "yyyy-MM-dd"))

# 5 Write DataFrame as CSV with different options (no merge condition)
# df.write \
#     .option("header", True) \
#     .option("delimiter", "|") \
#     .option("quote", '"') \
#     .option("quoteAll", True) \
#     .option("escape", "\\") \
#     .mode("overwrite") \
#     .csv("/Volumes/catalog_source/credit_card_schema/third")

