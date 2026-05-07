import pandas as pd
from pathlib import Path

raw_file = Path("data/raw/statlog+german+credit+data/german.data")
output_file = Path("data/raw/german_credit.csv")

columns = [
    "checking_account_status",
    "duration_months",
    "credit_history",
    "purpose",
    "credit_amount",
    "savings_account",
    "employment_since",
    "installment_rate",
    "personal_status_sex",
    "other_debtors",
    "present_residence_since",
    "property",
    "age",
    "other_installment_plans",
    "housing",
    "existing_credits",
    "job",
    "people_liable",
    "telephone",
    "foreign_worker",
    "risk_class"
]

df = pd.read_csv(raw_file, sep=r"\s+", header=None, names=columns)

df["Risk"] = df["risk_class"].map({1: "good", 2: "bad"})
df["Age"] = df["age"]
df["CreditAmount"] = df["credit_amount"]
df["Duration"] = df["duration_months"]
df["Purpose"] = df["purpose"]
df["Sex"] = df["personal_status_sex"]
df["Housing"] = df["housing"]
df["Job"] = df["job"]
df["SavingAccounts"] = df["savings_account"]
df["CheckingAccount"] = df["checking_account_status"]

final_df = df[
    [
        "Age",
        "CreditAmount",
        "Duration",
        "Purpose",
        "Sex",
        "Housing",
        "Job",
        "SavingAccounts",
        "CheckingAccount",
        "Risk",
    ]
]

final_df.to_csv(output_file, index=False)
print(f"Created {output_file} with {len(final_df)} rows.")
