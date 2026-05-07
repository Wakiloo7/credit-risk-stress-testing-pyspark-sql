import os
import random
import pandas as pd
from datetime import datetime, timedelta


RAW_PATH = "data/raw"


def generate_repayments(num_customers: int = 1000, max_loans_per_customer: int = 2) -> pd.DataFrame:
    rows = []
    repayment_id = 1

    for customer_id in range(1, num_customers + 1):
        loan_count = random.randint(1, max_loans_per_customer)

        for loan_index in range(1, loan_count + 1):
            loan_id = f"L{customer_id:05d}_{loan_index}"
            start_date = datetime(2021, 1, 1) + timedelta(days=random.randint(0, 800))
            installments = random.choice([6, 12, 18, 24, 36])

            for installment_no in range(1, installments + 1):
                due_date = start_date + timedelta(days=30 * installment_no)
                delay_days = max(0, int(random.gauss(3, 10)))
                paid_date = due_date + timedelta(days=delay_days)
                scheduled_amount = random.randint(100, 900)
                paid_amount = scheduled_amount if delay_days < 60 else random.randint(0, scheduled_amount)

                rows.append(
                    {
                        "repayment_id": repayment_id,
                        "customer_id": customer_id,
                        "loan_id": loan_id,
                        "installment_no": installment_no,
                        "due_date": due_date.strftime("%Y-%m-%d"),
                        "paid_date": paid_date.strftime("%Y-%m-%d"),
                        "scheduled_amount": scheduled_amount,
                        "paid_amount": paid_amount,
                        "days_past_due": delay_days,
                        "is_late_payment": 1 if delay_days > 15 else 0,
                        "is_default_event": 1 if delay_days >= 90 or paid_amount == 0 else 0,
                    }
                )
                repayment_id += 1

    return pd.DataFrame(rows)


def generate_macro_scenarios() -> pd.DataFrame:
    rows = []
    scenarios = [
        ("baseline", 0.04, 0.025, 0.03, 1.00),
        ("mild_stress", 0.065, 0.04, 0.045, 1.25),
        ("severe_stress", 0.10, 0.065, 0.065, 1.75),
    ]

    for scenario, unemployment_rate, inflation_rate, interest_rate, pd_multiplier in scenarios:
        rows.append(
            {
                "scenario_name": scenario,
                "unemployment_rate": unemployment_rate,
                "inflation_rate": inflation_rate,
                "interest_rate": interest_rate,
                "pd_multiplier": pd_multiplier,
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    os.makedirs(RAW_PATH, exist_ok=True)

    repayments = generate_repayments()
    macro = generate_macro_scenarios()

    repayments.to_csv(f"{RAW_PATH}/synthetic_repayments.csv", index=False)
    macro.to_csv(f"{RAW_PATH}/macroeconomic_scenarios.csv", index=False)

    print("Synthetic repayment and macroeconomic scenario files created.")


if __name__ == "__main__":
    main()