import unittest
from pyspark.sql import SparkSession
import file_path
from src.question_1.utils import (
    get_customer_bought_iphone13,
    get_customer_iphone13_14,
    get_customer_brought_all_models
)

class TestPurchaseFunctions(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("PurchaseUnitTests") \
            .getOrCreate()

        # Sample purchase data
        cls.purchase_data = [
            (1, "iphone13"),
            (1, "iphone14"),
            (2, "iphone13"),
            (3, "iphone14"),
            (4, "iphone13"),
            (4, "iphone14")
        ]
        cls.purchase_df = cls.spark.createDataFrame(cls.purchase_data, ["customer", "product_model"])

        # Product data
        cls.product_data = [
            ("iphone13",),
            ("iphone14",)
        ]
        cls.product_df = cls.spark.createDataFrame(cls.product_data, ["product_model"])

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_get_customer_bought_iphone13(self):
        result_df = get_customer_bought_iphone13(self.purchase_df)
        result_customers = [row.customer for row in result_df.collect()]
        self.assertTrue(all(model == "iphone13" for model in result_df.select("product_model").rdd.flatMap(lambda x: x).collect()))
        self.assertIn(1, result_customers)
        self.assertIn(2, result_customers)

    def test_get_customer_iphone13_14(self):
        result_df = get_customer_iphone13_14(self.purchase_df)
        customers = [row.customer for row in result_df.collect()]
        self.assertCountEqual(customers, [1, 4])  # Both bought iphone13 & iphone14

    def test_get_customer_brought_all_models(self):
        result_df = get_customer_brought_all_models(self.purchase_df, self.product_df)
        customers = [row.customer for row in result_df.collect()]
        self.assertCountEqual(customers, [1, 4])  # Only these bought all products

if __name__ == "__main__":
    unittest.main()
