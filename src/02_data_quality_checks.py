from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, sum as spark_sum, when
from utils.spark_session import get_spark_session


def check_nulls(df: DataFrame, table_name: str) -> None:
    total_rows = df.count()

    null_exprs = [
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ]

    result = df.select(null_exprs)
    print(f"\nNull check for {table_name}. Total rows: {total_rows}")
    result.show(truncate=False)


def check_duplicates(df: DataFrame, key_columns: list[str], table_name: str) -> None:
    duplicate_count = (
        df.groupBy(key_columns)
        .agg(count("*").alias("record_count"))
        .filter(col("record_count") > 1)
        .count()
    )

    print(f"Duplicate key check for {table_name}: {duplicate_count} duplicated keys found.")


def main() -> None:
    spark = get_spark_session("CreditRiskDataQuality")

    credit = spark.read.parquet("data/staging/stg_credit_applications")
    repayments = spark.read.parquet("data/staging/stg_repayments")
    scenarios = spark.read.parquet("data/staging/stg_macro_scenarios")

    check_nulls(credit, "stg_credit_applications")
    check_nulls(repayments, "stg_repayments")
    check_nulls(scenarios, "stg_macro_scenarios")

    check_duplicates(credit, ["customer_id"], "stg_credit_applications")
    check_duplicates(repayments, ["repayment_id"], "stg_repayments")
    check_duplicates(scenarios, ["scenario_name"], "stg_macro_scenarios")

    print("Data quality checks completed.")


if __name__ == "__main__":
    main()