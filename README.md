# Credit Risk Stress Testing Data Pipeline with PySpark and SQL

This project is an end-to-end financial data engineering pipeline built from scratch using **PySpark, SQL, Parquet, and Python**. It simulates a credit risk stress-testing workflow using the **UCI German Credit dataset**, synthetic repayment history, and macroeconomic stress scenarios.

The main goal of this project is to demonstrate how raw credit data can be ingested, validated, transformed, enriched with risk features, stress-tested under different economic scenarios, and converted into reporting-ready outputs for portfolio risk analysis.

---

## Project Overview

Financial institutions use credit risk stress testing to estimate how loan portfolios may behave under different economic conditions. This project simulates that process by creating a data pipeline that calculates risk indicators such as Exposure at Default (EAD), Probability of Default (PD), Loss Given Default (LGD), Expected Loss, risk segments, risk action categories, and portfolio impact under baseline, mild stress, and severe stress scenarios.

The project focuses on the **data engineering side** of credit risk analytics rather than building a complex statistical model. It demonstrates scalable data ingestion, cleaning, feature engineering, scenario-based transformation logic, and reporting output generation.

---

## Business Problem

A financial services team needs a reliable pipeline to process customer credit data and assess how portfolio risk changes under different economic stress scenarios. The pipeline answers questions such as what the total expected loss is under baseline, mild stress, and severe stress conditions; which customer segments contribute most to portfolio risk; which customers require immediate review under severe stress; how expected loss changes when probability of default and loss severity increase; and how raw credit and repayment data can be transformed into reporting-ready risk tables.

---

## Architecture

```text
Raw Data Sources
    |
    |-- UCI German Credit Data
    |-- Synthetic Repayment History
    |-- Macroeconomic Stress Scenarios
    |
    v
Staging Layer
    |
    |-- Credit Applications
    |-- Repayment Events
    |-- Scenario Assumptions
    |
    v
Data Quality Layer
    |
    |-- Null Checks
    |-- Duplicate Checks
    |-- Schema Validation
    |
    v
Feature Engineering Layer
    |
    |-- Repayment Behaviour Features
    |-- Credit Risk Segments
    |-- Base PD / LGD / EAD
    |
    v
Stress Testing Layer
    |
    |-- Baseline Scenario
    |-- Mild Stress Scenario
    |-- Severe Stress Scenario
    |-- Expected Loss Calculation
    |
    v
Reporting Layer
    |
    |-- Portfolio Stress Summary
    |-- Scenario Comparison
    |-- Risk Action Summary
    |-- Top High-Risk Customers
```

---

## Technologies Used

| Category | Tools |
|---|---|
| Programming | Python |
| Big Data Processing | PySpark |
| Query Logic | Spark SQL / SQL |
| Storage Format | Parquet, CSV |
| Data Quality | PySpark validation checks |
| Financial Analytics | Credit Risk, Stress Testing, Expected Loss |
| Version Control | Git and GitHub |
| Dataset | UCI German Credit Data + Synthetic Financial Data |

---

## Dataset

This project uses the **UCI German Credit dataset** as the base credit application dataset. Additional synthetic datasets are generated to make the project more realistic, including `synthetic_repayments.csv` for simulated repayment behaviour and late payment events, and `macroeconomic_scenarios.csv` for baseline, mild stress, and severe stress assumptions. The synthetic data is used only for portfolio demonstration and educational purposes.

---

## Project Structure

```text
credit-risk-stress-testing-pyspark-sql/

├── README.md
├── requirements.txt
├── .gitignore
├── config/
│   └── pipeline_config.yaml
├── data/
│   ├── raw/
│   │   ├── german_credit.csv
│   │   ├── synthetic_repayments.csv
│   │   └── macroeconomic_scenarios.csv
│   ├── staging/
│   ├── processed/
│   └── output/
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
├── sql/
│   ├── create_reporting_views.sql
│   └── risk_reporting_queries.sql
├── src/
│   ├── 00_generate_synthetic_data.py
│   ├── 01_ingest_raw_data.py
│   ├── 02_data_quality_checks.py
│   ├── 03_feature_engineering.py
│   ├── 04_stress_testing.py
│   ├── 05_reporting_outputs.py
│   └── utils/
│       └── spark_session.py
└── tests/
    └── test_data_quality.py
```

---

## Pipeline Steps

### 1. Synthetic Data Generation

The pipeline generates synthetic repayment history and macroeconomic stress scenarios to enrich the base credit dataset. Generated data includes loan repayment schedules, late payment indicators, default event indicators, baseline, mild stress, and severe stress scenarios, and probability of default multipliers.

```bash
python src/00_generate_synthetic_data.py
```

### 2. Raw Data Ingestion

The ingestion layer reads raw CSV files and writes them into the staging layer as Parquet files. Input files include `german_credit.csv`, `synthetic_repayments.csv`, and `macroeconomic_scenarios.csv`. Output staging tables include `stg_credit_applications`, `stg_repayments`, and `stg_macro_scenarios`.

```bash
python src/01_ingest_raw_data.py
```

### 3. Data Quality Checks

The data quality layer validates the staging datasets before transformation. Checks include null checks, duplicate key checks, row count validation, and basic schema consistency.

```bash
python src/02_data_quality_checks.py
```

### 4. Feature Engineering

The feature engineering layer creates customer-level credit risk features from credit application and repayment data. Features include late payment count, default event count, average days past due, maximum days past due, late payment ratio, repayment completion ratio, base probability of default, loss given default, exposure at default, and risk segment.

```bash
python src/03_feature_engineering.py
```

### 5. Stress Testing

The stress-testing layer applies macroeconomic scenario assumptions to the customer-level risk features. The project includes baseline, mild stress, and severe stress scenarios.

```text
Expected Loss = Probability of Default × Loss Given Default × Exposure at Default
```

```bash
python src/04_stress_testing.py
```

### 6. Reporting Outputs

The reporting layer generates CSV outputs that can be used for business reporting, dashboarding, or further analysis. Generated outputs include `portfolio_stress_summary`, `scenario_comparison`, `risk_action_summary`, `top_risk_customers`, and `customer_level_stress_results`.

```bash
python src/05_reporting_outputs.py
```

---

## Sample Results

### Scenario Comparison

| Scenario | Total Customers | Total EAD | Total Expected Loss | Average PD | Average LGD |
|---|---:|---:|---:|---:|---:|
| Severe Stress | 1000 | 3,271,258 | 302,114.01 | 0.1483 | 0.55 |
| Mild Stress | 1000 | 3,271,258 | 196,179.85 | 0.1059 | 0.50 |
| Baseline | 1000 | 3,271,258 | 141,248.16 | 0.0848 | 0.45 |

### Portfolio Stress Summary

| Scenario | Risk Segment | Customers | Total Exposure | Avg Stressed PD | Avg Stressed LGD | Total Expected Loss |
|---|---|---:|---:|---:|---:|---:|
| Baseline | High Risk | 300 | 1,181,438 | 0.1800 | 0.45 | 95,696.67 |
| Mild Stress | High Risk | 300 | 1,181,438 | 0.2250 | 0.50 | 132,912.19 |
| Severe Stress | High Risk | 300 | 1,181,438 | 0.3150 | 0.55 | 204,684.18 |

---

## How to Run the Project

### 1. Create a virtual environment

On Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 2. Install dependencies

```powershell
pip install -r requirements.txt
```

### 3. Run the pipeline

```powershell
python src/00_generate_synthetic_data.py
python src/01_ingest_raw_data.py
python src/02_data_quality_checks.py
python src/03_feature_engineering.py
python src/04_stress_testing.py
python src/05_reporting_outputs.py
```

### 4. View generated outputs

```powershell
Get-Content data\output\portfolio_stress_summary\part-*.csv -TotalCount 20
Get-Content data\output\scenario_comparison\part-*.csv -TotalCount 20
Get-Content data\output\top_risk_customers\part-*.csv -TotalCount 20
```

---

## Important Windows Setup Note

When running PySpark locally on Windows, Spark may require Hadoop utilities such as `winutils.exe`. If needed, configure the environment variables below:

```powershell
$env:HADOOP_HOME = "C:\hadoop"
${env:hadoop.home.dir} = "C:\hadoop"
$env:PATH = "C:\hadoop\bin;$env:PATH"
```

---

## SQL Reporting Layer

The `sql/` folder contains SQL scripts for reporting-style analysis. The `create_reporting_views.sql` file creates reporting views on top of final stress test results, while `risk_reporting_queries.sql` contains analytical queries for portfolio risk summaries, scenario comparison, and high-risk customer identification. These SQL files demonstrate how the final PySpark outputs can be consumed in a data warehouse or reporting environment.

---

## Key Skills Demonstrated

This project demonstrates end-to-end data pipeline development, PySpark-based data ingestion and transformation, Spark SQL analytics, data quality checks, feature engineering, financial stress-testing logic, credit risk metrics calculation, Parquet-based staging and processed layers, reporting-ready CSV outputs, portfolio risk analysis, and Git-based project structure and documentation.

---

## Future Improvements

Possible future enhancements include adding Airflow orchestration, Docker support, Great Expectations for advanced data quality validation, PostgreSQL or Snowflake as a warehouse target, Power BI dashboards, a machine learning model for probability of default prediction, MLflow experiment tracking, and a CI/CD pipeline with GitHub Actions.

---

## Disclaimer

This project is for educational and portfolio purposes. It uses public and synthetic data and does not represent a production-grade credit risk model or regulatory stress-testing framework.

---

## Author

**Md Wakil Ahmad**  
Email: wakil011152@gmail.com  
GitHub: https://github.com/Wakiloo7  
LinkedIn: https://www.linkedin.com/in/md-wakil-ahmad