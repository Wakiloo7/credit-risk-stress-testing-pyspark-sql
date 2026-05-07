-- Portfolio expected loss by scenario
SELECT
    scenario_name,
    COUNT(*) AS total_customers,
    ROUND(SUM(ead), 2) AS total_exposure,
    ROUND(SUM(expected_loss), 2) AS total_expected_loss,
    ROUND(SUM(expected_loss) / SUM(ead), 4) AS expected_loss_rate
FROM stress_test_results
GROUP BY scenario_name
ORDER BY total_expected_loss DESC;

-- Expected loss by customer risk segment
SELECT
    scenario_name,
    risk_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(ead), 2) AS total_exposure,
    ROUND(AVG(stressed_pd), 4) AS avg_pd,
    ROUND(AVG(stressed_lgd), 4) AS avg_lgd,
    ROUND(SUM(expected_loss), 2) AS total_expected_loss
FROM stress_test_results
GROUP BY scenario_name, risk_segment
ORDER BY scenario_name, total_expected_loss DESC;

-- Top customers under severe stress
SELECT
    customer_id,
    credit_amount,
    risk_segment,
    stressed_pd,
    stressed_lgd,
    ead,
    expected_loss,
    risk_action
FROM stress_test_results
WHERE scenario_name = 'severe_stress'
ORDER BY expected_loss DESC
LIMIT 50;