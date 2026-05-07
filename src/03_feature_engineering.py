from pyspark.sql.functions import (
    col,
    when,
    avg,
    max as spark_max,
    sum as spark_sum,
    count,
    round as spark_round,
)
from utils.spark_session import get_spark_session


def main() -> None:
    spark = get_spark_session("CreditRiskFeatureEngineering")

    credit = spark.read.parquet("data/staging/stg_credit_applications")
    repayments = spark.read.parquet("data/staging/stg_repayments")

    repayments_agg = (
        repayments
        .groupBy("customer_id")
        .agg(
            count("*").alias("total_installments"),
            spark_sum("is_late_payment").alias("late_payment_count"),
            spark_sum("is_default_event").alias("default_event_count"),
            avg("days_past_due").alias("avg_days_past_due"),
            spark_max("days_past_due").alias("max_days_past_due"),
            spark_sum("scheduled_amount").alias("total_scheduled_amount"),
            spark_sum("paid_amount").alias("total_paid_amount"),
        )
        .withColumn(
            "late_payment_ratio",
            spark_round(col("late_payment_count") / col("total_installments"), 4)
        )
        .withColumn(
            "repayment_completion_ratio",
            spark_round(col("total_paid_amount") / col("total_scheduled_amount"), 4)
        )
    )

    credit.createOrReplaceTempView("credit")
    repayments_agg.createOrReplaceTempView("repayment_features")

    features = spark.sql("""
        SELECT
            c.customer_id,
            c.Age AS age,
            c.CreditAmount AS credit_amount,
            c.Duration AS duration_months,
            c.Purpose AS loan_purpose,
            c.Sex AS sex,
            c.Housing AS housing,
            c.Job AS job_type,
            c.SavingAccounts AS saving_accounts,
            c.CheckingAccount AS checking_account,
            c.Risk AS risk_label,
            COALESCE(r.total_installments, 0) AS total_installments,
            COALESCE(r.late_payment_count, 0) AS late_payment_count,
            COALESCE(r.default_event_count, 0) AS default_event_count,
            COALESCE(r.avg_days_past_due, 0) AS avg_days_past_due,
            COALESCE(r.max_days_past_due, 0) AS max_days_past_due,
            COALESCE(r.late_payment_ratio, 0) AS late_payment_ratio,
            COALESCE(r.repayment_completion_ratio, 1) AS repayment_completion_ratio
        FROM credit c
        LEFT JOIN repayment_features r
            ON c.customer_id = r.customer_id
    """)

    scored_features = (
        features
        .withColumn(
            "base_pd",
            when(col("risk_label") == "bad", 0.18)
            .when(col("late_payment_ratio") > 0.30, 0.14)
            .when(col("max_days_past_due") >= 60, 0.12)
            .when(col("credit_amount") > 10000, 0.10)
            .otherwise(0.04)
        )
        .withColumn(
            "lgd",
            when(col("housing") == "own", 0.35)
            .when(col("housing") == "rent", 0.50)
            .otherwise(0.45)
        )
        .withColumn("ead", col("credit_amount"))
        .withColumn(
            "risk_segment",
            when(col("base_pd") >= 0.15, "High Risk")
            .when(col("base_pd") >= 0.08, "Medium Risk")
            .otherwise("Low Risk")
        )
    )

    scored_features.write.mode("overwrite").parquet("data/processed/customer_credit_features")

    print("Feature engineering completed.")


if __name__ == "__main__":
    main()