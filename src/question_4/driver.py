from utils import (get_schema,
                   dynamic_file,
                   flatten_json,
                   get_filter_record,
                   camel_to_snakecase,
                   add_current_date,
                   add_date_year_month)

option_dict ={
    "header":True,
    "multiLine":True
}
path = r"C:\Users\GowdhamanBJ\Desktop\pyspark_assignement\files\nested_json_file.json"
file_type = 'json'
schema = get_schema()

#Read JSON file provided in the attachment using the dynamic function
df = dynamic_file(path,file_type,option_dict,schema)
df.show()

#flatten json file
flatten_df = flatten_json(df)
flatten_df.show()

#3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)
count_without_flatten = df.count()
count_after_flatten = flatten_json(df).count()
print("count without flatten the json file: ",count_without_flatten)
print("count after flatten the json file: ",count_after_flatten)

#Filter the id which is equal to 1001
print("filter id 1001")
get_filter_record(flatten_df).show()

#convert the column names from camel case to snake case
print("camel case to snake case")
camel_to_snakecase(flatten_df).show()

print("add column load date")
flatten_df = add_current_date(flatten_df)
print("flatten_df")
flatten_df.show()

print("add date, month, year columns")
add_date_year_month(flatten_df).show()