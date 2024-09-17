# Bank Transactions with Balance Calculation (Explicit Schema)

## Description
This script processes a bank transaction dataset from an Excel file and calculates the current balance for each account based on its transaction history. The final output is stored in a CSV file format. In this version, the schema for the Excel file is explicitly enforced with the appropriated data types.

## Requirements
- PySpark
- pandas
- openpyxl
- OpenJDK 22.0.2

## Running the Code

1. Install the necessary dependencies:
    ```
    pip install pyspark
    ```
    
    ```
    pip install pandas
    ```

    ```
    pip install openpyxl
    ```

2. Run the PySpark script:
    ```
    spark-submit currentBalance.py
    ```

## Parameters
- `input_path`: The path to the Excel file with transactions.
- `output_path`: The path to the output file with Current Balance per Account Transaction.

## Output
The output will be saved in CSV format in the specified location.
