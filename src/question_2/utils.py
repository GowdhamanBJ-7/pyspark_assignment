from pyspark.sql.functions import udf, substring,expr,col,length,concat,lit
from pyspark.sql.types import StringType


# 5.Create a UDF to print only the last 4 digits marking the remaining digits as *
@udf(returnType=StringType())
def mask_four_digit(value):
    if value and len(value) >= 4:
        return "************" + value[-4:]
    return None

    # return concat(
    #     lit("**********"),
    #     substring(value, length(col(value) - 3), 4)
    # )
# Inside a UDF, you can’t use concat, lit, substring, or col directly
# — those are PySpark Column expressions, not plain Python values.


def get_masked_digit(df):
    return df.withColumn("masked",mask_four_digit(df["card_number"]))

#6
def masked_card(df):
        final_df = df.withColumn(
            "masked_card_number",
            concat(
                lit("************"),
                substring(col("card_number"), length(col("card_number")) - 3, 4)
            )
        )

        return final_df