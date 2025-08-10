import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from src.question3.utils import create_dynamic_column, get_no_of_action, convert_timeStamp_todate
import file_path

class TestUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("PySparkUnitTest") \
            .getOrCreate()

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
            StructField("login_id", IntegerType(), False),
            StructField("user_id", IntegerType(), False),
            StructField("action", StringType(), False),
            StructField("time_stamp", StringType(), False),
        ])

        cls.df = cls.spark.createDataFrame(data, schema)


    def test_create_dynamic_column(self):
        new_cols = ["log_id", "user_id", "user_activity", "time_stamp"]
        df_new = create_dynamic_column(self.df, new_cols)
        self.assertEqual(df_new.columns, new_cols)

    def test_convert_timeStamp(self):
        df_date = convert_timeStamp_todate(self.df)
        self.assertIn("login_date", df_date.columns)


if __name__ == "__main__":
    unittest.main()
