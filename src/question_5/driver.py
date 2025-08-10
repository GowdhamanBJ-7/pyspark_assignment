from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, avg, lit, current_date
from utils import *
import file_path

spark = SparkSession.builder.appName("employee").getOrCreate()
data1 = [
    (11, "james",  "D101", "ny", 9000, 34),
    (12, "michel", "D101", "ny", 8900, 32),
    (13, "robert", "D102", "ca", 7900, 29),
    (14, "scott",  "D103", "ca", 8000, 36),
    (15, "jen",    "D102", "ny", 9500, 38),
    (16, "jeff",   "D103", "uk", 9100, 35),
    (17, "maria",  "D101", "ny", 7900, 40)
]

data2 = [
    ("D101", "sales"),
    ("D102", "finance"),
    ("D103", "marketing"),
    ("D104", "hr"),
    ("D105", "support")
]

data3 = [
    ("ny", "newyork"),
    ("ca", "california"),
    ("uk", "russia")
]


# create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way
def create_schema(cols_types):
    return StructType([StructField(col_name, dtype, True) for col_name, dtype in cols_types])

employee_schema = create_schema([
    ("employee_id", IntegerType()),
    ("employee_name", StringType()),
    ("department", StringType()),
    ("State", StringType()),
    ("salary", IntegerType()),
    ("Age", IntegerType())
])

department_schema = create_schema([
    ("dept_id", StringType()),
    ("dept_name", StringType())
])

country_schema = create_schema([
    ("country_code", StringType()),
    ("country_name", StringType())
])

employee_df = spark.createDataFrame(data1, schema=employee_schema)
department_df = spark.createDataFrame(data2, schema=department_schema)
country_df = spark.createDataFrame(data3, schema=country_schema)


#averge salary of employee
get_avg_salary(employee_df).show()

#Find the employee’s name and department name whose name starts with ‘m’
get_dept_name_m(employee_df,department_df).show()

#Create another new column in  employee_df as a bonus by multiplying employee salary *2
get_bonus_col(employee_df).show()

#reorder employee df
reordered_df = employee_df.select("employee_id", "employee_name", "salary", "State", "Age", "department")
reordered_df.show()

#  Give the result of an inner join, left join, and right join
#  when joining employee_df with department_df in a dynamic way
print("dynamic join")
join_results = dynamic_join(employee_df, department_df)

for join_type, df in join_results.items():
    print(f"\n {join_type.upper()} join")
    df.show()

#  Derive a new data frame with country_name instead of State in employee_df
country_replace_df  = join_employee_country(employee_df,country_df)
country_replace_df.show()

# convert all the column names into lowercase from the result of question 7in a dynamic way,
# add the load_date column with the current date
print("lower case column with current date")
lowercase_df = replace_lowercase(country_replace_df)
lowercase_df.show()

# lowercase_df.write.mode("overwrite").option("path", "employee_parquet").format("parquet").saveAsTable("mydb.employee_parquet")
# lowercase_df.write.mode("overwrite").option("path", "employee_csv").format("csv").option("header", True).saveAsTable("mydb.employee_csv")