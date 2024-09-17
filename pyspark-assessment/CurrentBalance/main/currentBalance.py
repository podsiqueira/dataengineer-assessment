from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window as W
import pandas as pd
from pathlib import Path

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("CurrentBalance") \
    .getOrCreate()

current_dir = Path(__file__).resolve().parent
parent_dir = current_dir.parent
input_path =  parent_dir / "input/EmbeddedFile.xlsm"
output_path =  parent_dir / "output/"

def clean_column_spaces(df: DataFrame) -> DataFrame:
    """
    This function takes a DataFrame and returns a new DataFrame with
    all columns cleaned by removing leading and trailing spaces, 
    and replacing multiple spaces within the strings with a single space.
    
    :param df: Input DataFrame with columns to clean
    :return: New DataFrame with cleaned columns
    """
    cleaned_df = df.select(
        *[F.trim(F.regexp_replace(F.col(c), "\s+", " ")).alias(c) for c in df.columns]
    )
    return cleaned_df

df_raw = pd.read_excel(input_path)



df_DataFrame = spark.createDataFrame(df_raw)

df_clean = clean_column_spaces(df_DataFrame)

df_schema = df_clean \
    .withColumn("TransactionDate", F.to_date(df_clean.TransactionDate.cast("string"), "yyyyMMdd")) \
    .withColumn("AccountNumber", df_clean.AccountNumber.cast(T.LongType())) \
    .withColumn("Amount", df_clean.Amount.cast(T.DoubleType()))
df_schema.show()
df_schema.printSchema()

df_transAmount = df_schema \
.withColumn("TransactionAmount", F.when(F.col("TransactionType")=='Credit', F.col("Amount")).otherwise(-F.col("Amount")))

df_transAmount.show()

df_currentBalance = df_transAmount.withColumn("CurrentBalance", F.sum(F.col("TransactionAmount")).over(W.partitionBy("AccountNumber").orderBy("TransactionDate")))

df = df_currentBalance.drop(F.col("TransactionAmount"))

df.show(truncate=False)

df \
    .write \
        .format("csv") \
            .options(header='True', delimiter=',') \
                .mode('overwrite').save(str(output_path))