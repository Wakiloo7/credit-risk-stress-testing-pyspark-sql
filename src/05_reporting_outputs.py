from utils.spark_session import get_spark_session


def main() -> None:
    spark = get_spark_session("CreditRiskReporting")

    results = spark.read.parquet("data/processed/stress_test_results")
    results.createOrReplaceTempView("stress_test_results")

    top_risk_customers = spark.sql("""
        SELECT
            customer_id,
            scenario_name,
            risk_segment,
            credit_amount,
            stressed_pd,
            stressed_lgd,
            ead,
            expected_loss,
            risk_action
        FROM stress_test_results
        WHERE scenario_name = 'severe_stress'
        ORDER BY expected_loss DESC
        LIMIT 50
    """)

    scenario_comparison = spark.sql("""
        SELECT
            scenario_name,
            COUNT(*) AS total_customers,
            ROUND(SUM(ead), 2) AS total_ead,
            ROUND(SUM(expected_loss), 2) AS total_expected_loss,
            ROUND(AVG(stressed_pd), 4) AS average_pd,
            ROUND(AVG(stressed_lgd), 4) AS average_lgd
        FROM stress_test_results
        GROUP BY scenario_name
        ORDER BY total_expected_loss DESC
    """)

    risk_action_summary = spark.sql("""
        SELECT
            scenario_name,
            risk_action,
            COUNT(*) AS customer_count,
            ROUND(SUM(expected_loss), 2) AS expected_loss
        FROM stress_test_results
        GROUP BY scenario_name, risk_action
        ORDER BY scenario_name, expected_loss DESC
    """)

    top_risk_customers.write.mode("overwrite").csv("data/output/top_risk_customers", header=True)
    scenario_comparison.write.mode("overwrite").csv("data/output/scenario_comparison", header=True)
    risk_action_summary.write.mode("overwrite").csv("data/output/risk_action_summary", header=True)

    print("Reporting outputs created.")


if __name__ == "__main__":
    main()