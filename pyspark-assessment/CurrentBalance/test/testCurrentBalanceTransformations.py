import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W
import pandas as pd

from main.currentBalance import clean_column_spaces  # Import the function from the main script

@pytest.fixture()
def spark():
    """
    Using a pytest fixture allows us to share a spark session across tests, 
    tearing it down when the tests are complete.
    """
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("TestCurrentBalance") \
        .getOrCreate()
    yield spark
    
def test_clean_column_spaces(spark):
    """
    Test case to verify that the clean_column_spaces function will
    trim and remove spaces in the columns.
    """
    # Create a DataFrame with some data having leading/trailing and multiple spaces
    data = [("John    Denis ", " 100 "), (" Bob   Taylor", "200")]
    schema = ["Name", "Amount"]
    df = spark.createDataFrame(data, schema)

    # Call the function to clean column spaces
    df_clean = clean_column_spaces(df)

    # Collect the result
    result = df_clean.collect()

    # Expected result after cleaning
    expected = [("John Denis", "100"), ("Bob Taylor", "200")]

    # Assert that the results are as expected
    assert result == expected 
    
def tes_current_balance_transaction(spark):
    """
    Test case to verify that the current balance calculation is correct.
    """
    # Create a sample DataFrame
    data = [("2023-01-01", 123456, 100.5, "Credit"),
            ("2023-01-02", 123456, 200.0, "Debit"),
            ("2023-01-03", 123456, 150.0, "Credit")]

    schema = T.StructType([
        T.StructField("TransactionDate", T.DateType(), True),
        T.StructField("AccountNumber", T.LongType(), True),
        T.StructField("Amount", T.DoubleType(), True),
        T.StructField("TransactionType", T.StringType(), True),
    ])

    df = spark.createDataFrame(data, schema)

    # Apply transformation to calculate TransactionAmount and CurrentBalance
    df_transAmount = df.withColumn(
        "TransactionAmount",
        F.when(F.col("TransactionType") == 'Credit', F.col("Amount"))
        .otherwise(-F.col("Amount"))
    )
    
    df_currentBalance = df_transAmount.withColumn(
        "CurrentBalance", 
        F.sum(F.col("TransactionAmount")).over(W.partitionBy("AccountNumber").orderBy("TransactionDate"))
    )

    # Collect the result
    result = df_currentBalance.select("TransactionDate", "AccountNumber", "TransactionAmount", "CurrentBalance").collect()

    # Expected results
    expected = [
        (pd.to_datetime("2023-01-01"), 123456, 100.5, 100.5),
        (pd.to_datetime("2023-01-02"), 123456, -200.0, -99.5),
        (pd.to_datetime("2023-01-03"), 123456, 150.0, 50.5)
    ]

    # Assert the balance calculation is correct
    for row, exp in zip(result, expected):
        assert row["TransactionDate"] == exp[0]
        assert row["AccountNumber"] == exp[1]
        assert row["TransactionAmount"] == exp[2]
        assert row["CurrentBalance"] == exp[3]