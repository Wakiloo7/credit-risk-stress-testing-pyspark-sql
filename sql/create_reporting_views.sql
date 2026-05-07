-- ============================================================
-- Credit Risk Stress Testing Reporting Views
-- Project: credit-risk-stress-testing-pyspark-sql
-- Purpose: Create reporting-ready views for portfolio risk analysis
-- ============================================================

-- NOTE:
-- These views assume that the final table/view is called:
-- stress_test_results
--
-- In local PySpark, the table is created as a temporary Spark SQL view.
-- In a warehouse/database environment, you can materialize the final
-- PySpark output and then create these views on top of it.

-- ============================================================
-- 1. Portfolio Risk Summary by Scenario
-- ============================================================

CREATE OR REPLACE VIEW vw_portfolio_risk_summary AS
SELECT
    scenario_name,
    COUNT(*) AS total_customers,
    ROUND(SUM(ead), 2) AS total_exposure,
    ROUND(AVG(stressed_pd), 4) AS average_probability_of_default,
    ROUND(AVG(stressed_lgd), 4) AS average_loss_given_default,
    ROUND(SUM(expected_loss), 2) AS total_expected_loss,
    ROUND(SUM(expected_loss) / NULLIF(SUM(ead), 0), 4) AS expected_loss_rate
FROM stress_test_results
GROUP BY scenario_name;


-- ============================================================
-- 2. Risk Segment Summary by Scenario
-- ============================================================

CREATE OR REPLACE VIEW vw_risk_segment_summary AS
SELECT
    scenario_name,
    risk_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(ead), 2) AS total_exposure,
    ROUND(AVG(stressed_pd), 4) AS average_stressed_pd,
    ROUND(AVG(stressed_lgd), 4) AS average_stressed_lgd,
    ROUND(SUM(expected_loss), 2) AS total_expected_loss,
    ROUND(SUM(expected_loss) / NULLIF(SUM(ead), 0), 4) AS expected_loss_rate
FROM stress_test_results
GROUP BY
    scenario_name,
    risk_segment;


-- ============================================================
-- 3. High-Risk Customer View
-- ============================================================

CREATE OR REPLACE VIEW vw_high_risk_customers AS
SELECT
    customer_id,
    scenario_name,
    age,
    loan_purpose,
    housing,
    job_type,
    risk_label,
    risk_segment,
    credit_amount,
    ead,
    base_pd,
    stressed_pd,
    stressed_lgd,
    expected_loss,
    risk_action,
    late_payment_count,
    default_event_count,
    avg_days_past_due,
    max_days_past_due,
    late_payment_ratio,
    repayment_completion_ratio
FROM stress_test_results
WHERE risk_action IN ('Review Immediately', 'Monitor Closely');


-- ============================================================
-- 4. Severe Stress Top Exposure View
-- ============================================================

CREATE OR REPLACE VIEW vw_severe_stress_top_exposure AS
SELECT
    customer_id,
    scenario_name,
    risk_segment,
    credit_amount,
    ead,
    stressed_pd,
    stressed_lgd,
    expected_loss,
    risk_action
FROM stress_test_results
WHERE scenario_name = 'severe_stress'
ORDER BY expected_loss DESC;


-- ============================================================
-- 5. Scenario Comparison View
-- ============================================================

CREATE OR REPLACE VIEW vw_scenario_comparison AS
SELECT
    scenario_name,
    COUNT(*) AS total_customers,
    ROUND(SUM(ead), 2) AS total_ead,
    ROUND(SUM(expected_loss), 2) AS total_expected_loss,
    ROUND(AVG(stressed_pd), 4) AS average_pd,
    ROUND(AVG(stressed_lgd), 4) AS average_lgd,
    ROUND(MAX(expected_loss), 2) AS max_customer_expected_loss
FROM stress_test_results
GROUP BY scenario_name;


-- ============================================================
-- 6. Risk Action Summary View
-- ============================================================

CREATE OR REPLACE VIEW vw_risk_action_summary AS
SELECT
    scenario_name,
    risk_action,
    COUNT(*) AS customer_count,
    ROUND(SUM(ead), 2) AS total_exposure,
    ROUND(SUM(expected_loss), 2) AS total_expected_loss
FROM stress_test_results
GROUP BY
    scenario_name,
    risk_action;