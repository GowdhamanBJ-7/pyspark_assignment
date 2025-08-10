import unittest
from pyspark.sql import SparkSession
from src.question_2.utils import get_masked_digit, masked_card
import file_path

class MaskingTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("MaskingTests").getOrCreate()

    def test_get_masked_digit(self):
        df = self.spark.createDataFrame(
            [("1234567891234567",), ("5678912345671234",)],
            ["card_number"]
        )

        # Apply function
        result_df = get_masked_digit(df).collect()

        # Assertions
        expected = ["************4567", "************1234"]
        actual = [row.masked for row in result_df]
        self.assertEqual(expected, actual)

    def test_masked_card(self):
        # Input DataFrame
        df = self.spark.createDataFrame(
            [("9123456712345678",)],
            ["card_number"]
        )

        # Apply function
        result_df = masked_card(df).collect()

        # Assertions
        self.assertEqual(result_df[0].masked_card_number, "************5678")


if __name__ == '__main__':
    unittest.main()
