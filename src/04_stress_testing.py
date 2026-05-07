from pyspark.sql.functions import col, round as spark_round, when
from utils.spark_session import get_spark_session


def main() -> None:
    spark = get_spark_session("CreditRiskStressTesting")

    features = spark.read.parquet("data/processed/customer_credit_features")
    scenarios = spark.read.parquet("data/staging/stg_macro_scenarios")

    features.createOrReplaceTempView("customer_credit_features")
    scenarios.createOrReplaceTempView("macro_scenarios")

    stressed = spark.sql("""
        SELECT
            f.customer_id,
            f.age,
            f.credit_amount,
            f.duration_months,
            f.loan_purpose,
            f.housing,
            f.job_type,
            f.risk_label,
            f.total_installments,
            f.late_payment_count,
            f.default_event_count,
            f.avg_days_past_due,
            f.max_days_past_due,
            f.late_payment_ratio,
            f.repayment_completion_ratio,
            f.base_pd,
            f.lgd,
            f.ead,
            f.risk_segment,
            s.scenario_name,
            s.unemployment_rate,
            s.inflation_rate,
            s.interest_rate,
            s.pd_multiplier
        FROM customer_credit_features f
        CROSS JOIN macro_scenarios s
    """)

    stressed = (
        stressed
        .withColumn(
            "stressed_pd",
            spark_round(
                when(col("base_pd") * col("pd_multiplier") > 1.0, 1.0)
                .otherwise(col("base_pd") * col("pd_multiplier")),
                4,
            )
        )
        .withColumn(
            "stressed_lgd",
            spark_round(
                when(col("scenario_name") == "severe_stress", col("lgd") + 0.10)
                .when(col("scenario_name") == "mild_stress", col("lgd") + 0.05)
                .otherwise(col("lgd")),
                4,
            )
        )
        .withColumn(
            "expected_loss",
            spark_round(col("stressed_pd") * col("stressed_lgd") * col("ead"), 2)
        )
        .withColumn(
            "risk_action",
            when(col("expected_loss") >= 2000, "Review Immediately")
            .when(col("expected_loss") >= 750, "Monitor Closely")
            .otherwise("Standard Monitoring")
        )
    )

    stressed.createOrReplaceTempView("stress_test_results")

    summary = spark.sql("""
        SELECT
            scenario_name,
            risk_segment,
            COUNT(*) AS customer_count,
            ROUND(SUM(ead), 2) AS total_exposure,
            ROUND(AVG(stressed_pd), 4) AS avg_stressed_pd,
            ROUND(AVG(stressed_lgd), 4) AS avg_stressed_lgd,
            ROUND(SUM(expected_loss), 2) AS total_expected_loss,
            ROUND(SUM(expected_loss) / SUM(ead), 4) AS expected_loss_rate
        FROM stress_test_results
        GROUP BY scenario_name, risk_segment
        ORDER BY scenario_name, total_expected_loss DESC
    """)

    stressed.write.mode("overwrite").parquet("data/processed/stress_test_results")
    summary.write.mode("overwrite").csv("data/output/portfolio_stress_summary", header=True)
    stressed.write.mode("overwrite").csv("data/output/customer_level_stress_results", header=True)

    print("Stress testing completed. Output tables created.")


if __name__ == "__main__":
    main()