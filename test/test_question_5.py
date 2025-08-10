import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import date
import file_path

from src.question_5.utils import (
    get_avg_salary,get_dept_name_m,get_bonus_col,dynamic_join,join_employee_country,replace_lowercase
)

class EmployeeUtilsTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .appName("EmployeeUtilsTests") \
            .getOrCreate()

        employee_data = [
            (11, "james",  "D101", "ny", 9000, 34),
            (12, "michel", "D101", "ny", 8900, 32),
            (13, "robert", "D102", "ca", 7900, 29),
            (17, "maria",  "D101", "ny", 7900, 40)
        ]
        employee_cols = ["employee_id", "employee_name", "department", "State", "salary", "Age"]
        cls.employee_df = cls.spark.createDataFrame(employee_data, employee_cols)

        department_data = [
            ("D101", "sales"),
            ("D102", "finance"),
            ("D103", "marketing")
        ]
        department_cols = ["dept_id", "dept_name"]
        cls.department_df = cls.spark.createDataFrame(department_data, department_cols)

        country_data = [
            ("ny", "newyork"),
            ("ca", "california")
        ]
        country_cols = ["country_code", "country_name"]
        cls.country_df = cls.spark.createDataFrame(country_data, country_cols)

    def test_get_avg_salary(self):
        result_df = get_avg_salary(self.employee_df).collect()
        dept_dict = {row["department"]: row["avg_salary"] for row in result_df}
        self.assertAlmostEqual(dept_dict["D101"], (9000 + 8900 + 7900) / 3)
        self.assertAlmostEqual(dept_dict["D102"], 7900)

    def test_get_dept_name_m(self):
        result_df = get_dept_name_m(self.employee_df, self.department_df).collect()
        names = [row["employee_name"] for row in result_df]
        self.assertIn("michel", names)
        self.assertIn("maria", names)

    def test_get_bonus_col(self):
        result_df = get_bonus_col(self.employee_df).collect()
        for row in result_df:
            self.assertEqual(row["bonus"], row["salary"] * 2)

    def test_dynamic_join(self):
        join_results = dynamic_join(self.employee_df, self.department_df)
        self.assertIn("inner", join_results)
        self.assertIn("left", join_results)
        self.assertIn("right", join_results)

        inner_count = join_results["inner"].count()
        left_count = join_results["left"].count()
        self.assertLessEqual(inner_count, left_count)

    def test_join_employee_country(self):
        result_df = join_employee_country(self.employee_df, self.country_df).collect()
        states = [row["country_name"] for row in result_df]
        self.assertIn("newyork", states)
        self.assertIn("california", states)

    def test_replace_lowercase(self):
        country_replace_df = join_employee_country(self.employee_df, self.country_df)
        lowercase_df = replace_lowercase(country_replace_df).collect()
        cols_lowercase = [c.lower() for c in country_replace_df.columns] + ["load_date"]
        self.assertListEqual(sorted(lowercase_df[0].asDict().keys()), sorted(cols_lowercase))
        # Ensure load_date is today's date for all rows
        for row in lowercase_df:
            self.assertEqual(str(row["load_date"]), str(date.today()))

if __name__ == "__main__":
    unittest.main()
