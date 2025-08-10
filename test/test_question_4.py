import unittest
from datetime import date
from pyspark.sql import SparkSession
import file_path
from src.question_4.utils import (
    get_schema,
    flatten_json,
    get_filter_record,
    camel_to_snakecase,
    add_current_date,
    add_date_year_month
)

class ReadJsonModuleTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("ReadJsonTests") \
            .getOrCreate()

        # Create in-memory DataFrame equivalent to your JSON
        cls.sample_data = [
            {
                "id": 1001,
                "properties": {
                    "name": "StoreA",
                    "storeSize": "Large"
                },
                "employees": [
                    {"empId": 1001, "empName": "John"},
                    {"empId": 1002, "empName": "Mary"}
                ]
            },
            {
                "id": 1002,
                "properties": {
                    "name": "StoreB",
                    "storeSize": "Small"
                },
                "employees": [
                    {"empId": 1001, "empName": "Alice"}
                ]
            }
        ]

        schema = get_schema()
        cls.df = cls.spark.createDataFrame(cls.sample_data, schema=schema)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_flatten_json(self):
        flat_df = flatten_json(self.df)
        self.assertIn("empId", flat_df.columns)
        self.assertIn("empName", flat_df.columns)
        self.assertEqual(flat_df.count(), 3)  # 2 + 1 employees

    def test_get_filter_record(self):
        flat_df = flatten_json(self.df)
        filtered_df = get_filter_record(flat_df)
        ids = [row["empId"] for row in filtered_df.collect()]
        self.assertTrue(all(emp_id == 1001 for emp_id in ids))

    def test_camel_to_snakecase(self):
        flat_df = flatten_json(self.df)
        snake_df = camel_to_snakecase(flat_df)
        self.assertTrue(all("_" in c or c.islower() for c in snake_df.columns))

    def test_add_current_date(self):
        flat_df = flatten_json(self.df)
        dated_df = add_current_date(flat_df)
        self.assertIn("load_date", dated_df.columns)
        self.assertEqual(dated_df.select("load_date").distinct().count(), 1)

    def test_add_date_year_month(self):
        flat_df = flatten_json(self.df)
        dated_df = add_current_date(flat_df)
        final_df = add_date_year_month(dated_df)
        self.assertIn("year", final_df.columns)
        self.assertIn("month", final_df.columns)
        self.assertIn("date", final_df.columns)
        today = date.today()
        row = final_df.select("year", "month", "date").first()
        self.assertEqual(row["year"], today.year)
        self.assertEqual(row["month"], today.month)
        self.assertEqual(row["date"], today.day)


if __name__ == "__main__":
    unittest.main()
