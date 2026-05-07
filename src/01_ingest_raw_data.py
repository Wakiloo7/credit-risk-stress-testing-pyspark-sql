from pyspark.sql.functions import col, monotonically_increasing_id
from utils.spark_session import get_spark_session


def main() -> None:
    spark = get_spark_session("CreditRiskIngestion")

    credit = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw/german_credit.csv")
    )

    repayments = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw/synthetic_repayments.csv")
    )

    scenarios = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv("data/raw/macroeconomic_scenarios.csv")
    )

    credit = credit.withColumn("customer_id", monotonically_increasing_id() + 1)

    credit.write.mode("overwrite").parquet("data/staging/stg_credit_applications")
    repayments.write.mode("overwrite").parquet("data/staging/stg_repayments")
    scenarios.write.mode("overwrite").parquet("data/staging/stg_macro_scenarios")

    print("Raw data ingested into staging parquet layers.")


if __name__ == "__main__":
    main()