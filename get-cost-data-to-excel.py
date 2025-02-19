import boto3
import calendar
import pandas as pd
from datetime import datetime, timedelta

# Get the current date
current_date = datetime.today()

# Calculate the first day of the current month
first_day_of_current_month = current_date.replace(day=1)

# Calculate the first day of the previous month
first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)

# Set the start and end dates for the report
start_date = first_day_of_previous_month.strftime("%Y-%m-%d") # or replace with a specific date
end_date = first_day_of_current_month.strftime("%Y-%m-%d") # or replace with a specific date

# List of AWS CLI profile names (replace these with your actual names)
aws_accounts = ["account-1", "account-2"]

# Initialize dictionary to hold cost data for each account
all_account_data = {}

for account in aws_accounts:
    # Set the AWS profile for the current account
    boto3.setup_default_session(profile_name=account)

    # Initialize AWS Cost Explorer client for the current profile
    client = boto3.client('ce')

    # Fetch cost data grouped by service and month
    response = client.get_cost_and_usage(
        TimePeriod={"Start": start_date, "End": end_date},
        Granularity="MONTHLY",
        Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}]
    )

    # Process response into a structured format
    cost_data = {}
    months = []

    for result in response["ResultsByTime"]:
        period_start = result["TimePeriod"]["Start"]
        month_label = datetime.strptime(period_start, "%Y-%m-%d").strftime("%b '%y")  # Format: Dec '24
        months.append(month_label)

        for group in result["Groups"]:
            service = group["Keys"][0]
            cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
            if service not in cost_data:
                cost_data[service] = {}
            cost_data[service][month_label] = cost

    # Convert to DataFrame
    df = pd.DataFrame(cost_data).T.fillna(0)  # Transpose to make services rows
    df = df.reindex(columns=months).reset_index()
    df.rename(columns={"index": "Service"}, inplace=True)

    # Add a Total column and sort by it
    df["Total"] = df[months].sum(axis=1)
    df = df.sort_values(by="Total", ascending=False).drop(columns=["Total"])  # Drop 'Total' after sorting

    # Store the result for the current account in the all_account_data dictionary
    all_account_data[account] = df

    # Output
    print(f"Cost data for account '{account}' fetched successfully.")

# Write to Excel
with pd.ExcelWriter("aws_cost_data.xlsx", engine='openpyxl') as writer:
    for account, df in all_account_data.items():
        df.to_excel(writer, sheet_name=account, index=False)

print("Excel file updated successfully, sorted by highest cost.")
