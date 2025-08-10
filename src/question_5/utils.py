from pyspark.sql.functions import avg,col, current_date

#Find the average salary of each department
def get_avg_salary(employee_df):
     return employee_df.groupBy("department").agg(avg("salary").alias("avg_salary"))

#Find the employee’s name and department name whose name starts with ‘m’
def get_dept_name_m(employee_df, department_df):
     m_name_df = (
          employee_df.join(department_df, employee_df.department == department_df.dept_id, "inner")
          .filter(col("employee_name").startswith("m"))
          .select("employee_name", "dept_name")
     )
     return m_name_df

#bonus * 2
def get_bonus_col(df):
     return df.withColumn("bonus", df["salary"]*2)

#joins
def dynamic_join(employee_df, department_df):
    join_types = ["inner", "left", "right"]
    results = {}
    for join_type in join_types:
        results[join_type] = employee_df.join(
            department_df, employee_df.department == department_df.dept_id, how=join_type
        )
    return results

# Derive a new data frame with country_name instead of State in employee_df
def join_employee_country(employee_df,country_df):
     return employee_df.join(country_df, employee_df.State == country_df.country_code, how="inner")

def replace_lowercase(country_replace_df):
    lowercase_cols = [col(c).alias(c.lower()) for c in country_replace_df.columns]
    lowercase_df = country_replace_df.select(*lowercase_cols).withColumn("load_date", current_date())
    return lowercase_df