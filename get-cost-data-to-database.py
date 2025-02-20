import boto3
import psycopg2
import time
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

# Setup logging
logging.basicConfig(
    level=logging.INFO,  # Log all levels from INFO and above
    format="%(asctime)s - %(levelname)s - %(message)s",  # Timestamp and severity level
    handlers=[
        logging.StreamHandler(),  # Output to console
        logging.FileHandler("aws_costs_script.log")  # Log to file
    ]
)
logger = logging.getLogger()

# Database connection parameters
db_host = 'db_host'
db_name = 'db_name'
db_user = 'db_user'
db_password = 'db_password'

# Connect to the PostgreSQL database
def connect_db():
    try:
        conn = psycopg2.connect(
            host=db_host,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        logger.info("Database connection established successfully.")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise

conn = connect_db()
cursor = conn.cursor()

# Create the necessary tables if they don't exist
cursor.execute("""
    CREATE TABLE IF NOT EXISTS accounts (
        account_id SERIAL PRIMARY KEY,
        account_name VARCHAR(255) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS months (
        month_id SERIAL PRIMARY KEY,
        month_name VARCHAR(20) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS years (
        year_id SERIAL PRIMARY KEY,
        year_name VARCHAR(4) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS services (
        service_id SERIAL PRIMARY KEY,
        service_name VARCHAR(255) UNIQUE NOT NULL
    );

    CREATE TABLE IF NOT EXISTS aws_costs (
        cost_id SERIAL PRIMARY KEY,
        account_id INT REFERENCES accounts(account_id),
        service_id INT REFERENCES services(service_id),
        month_id INT REFERENCES months(month_id),
        year_id INT REFERENCES years(year_id),
        cost DECIMAL(18, 2) NOT NULL,
        UNIQUE(account_id, service_id, month_id, year_id)
    );
""")
conn.commit()
logger.info("Database schema created or verified.")

# Batch insert function for accounts, months, years, and services
def batch_insert(table, column, values):
    try:
        cursor.executemany(f"""
            INSERT INTO {table} ({column})
            VALUES (%s)
            ON CONFLICT ({column}) DO NOTHING
        """, [(value,) for value in values])
        conn.commit()
        logger.info(f"Inserted {len(values)} records into {table}.")
    except Exception as e:
        logger.error(f"Error inserting into {table}: {e}")
        conn.rollback()

# Get or insert month
def get_or_insert_month(month_name):
    cursor.execute("""
        SELECT month_id FROM months WHERE month_name = %s
    """, (month_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("""
            INSERT INTO months (month_name) VALUES (%s) RETURNING month_id
        """, (month_name,))
        conn.commit()
        return cursor.fetchone()[0]

# Get or insert year
def get_or_insert_year(year_name):
    cursor.execute("""
        SELECT year_id FROM years WHERE year_name = %s
    """, (year_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("""
            INSERT INTO years (year_name) VALUES (%s) RETURNING year_id
        """, (year_name,))
        conn.commit()
        return cursor.fetchone()[0]

# Get or insert service
def get_or_insert_service(service_name):
    cursor.execute("""
        SELECT service_id FROM services WHERE service_name = %s
    """, (service_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("""
            INSERT INTO services (service_name) VALUES (%s) RETURNING service_id
        """, (service_name,))
        conn.commit()
        return cursor.fetchone()[0]

# Get or insert account
def get_or_insert_account(account_name):
    cursor.execute("""
        SELECT account_id FROM accounts WHERE account_name = %s
    """, (account_name,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute("""
            INSERT INTO accounts (account_name) VALUES (%s) RETURNING account_id
        """, (account_name,))
        conn.commit()
        return cursor.fetchone()[0]

# Fetch cost data and insert it
def process_account(account):
    try:
        logger.info(f"Processing account '{account}'.")

        # Set the AWS profile for the current account
        boto3.setup_default_session(profile_name=account)

        # Initialize AWS Cost Explorer client for the current profile
        client = boto3.client('ce')

        # Retry logic for AWS API
        retries = 3
        for attempt in range(retries):
            try:
                response = client.get_cost_and_usage(
                    TimePeriod={"Start": start_date, "End": end_date},
                    Granularity="MONTHLY",
                    Metrics=["UnblendedCost"],
                    GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}]
                )
                logger.info(f"AWS API response fetched for account '{account}'.")
                break  # If successful, exit the retry loop
            except Exception as e:
                if attempt < retries - 1:
                    logger.warning(f"AWS API call failed (attempt {attempt + 1}/{retries}): {e}. Retrying...")
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logger.error(f"AWS API call failed after {retries} attempts for account '{account}': {e}")
                    return

        # Get month and year details
        period_start = response["ResultsByTime"][0]["TimePeriod"]["Start"]
        month_label = datetime.strptime(period_start, "%Y-%m-%d").strftime("%b")
        year_label = datetime.strptime(period_start, "%Y-%m-%d").strftime("%Y")

        # Insert months and years in bulk
        batch_insert('months', 'month_name', [month_label])
        batch_insert('years', 'year_name', [year_label])

        month_id = get_or_insert_month(month_label)
        year_id = get_or_insert_year(year_label)

        service_names = set()  # To avoid duplicates
        for result in response["ResultsByTime"]:
            for group in result["Groups"]:
                service_name = group["Keys"][0]
                service_names.add(service_name)

        # Insert services in bulk
        batch_insert('services', 'service_name', list(service_names))

        # Insert costs in bulk
        cost_data = []
        for result in response["ResultsByTime"]:
            for group in result["Groups"]:
                service_name = group["Keys"][0]
                cost = float(group["Metrics"]["UnblendedCost"]["Amount"])
                service_id = get_or_insert_service(service_name)
                account_id = get_or_insert_account(account)

                cost_data.append((account_id, service_id, month_id, year_id, cost))

        cursor.executemany("""
            INSERT INTO aws_costs (account_id, service_id, month_id, year_id, cost)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (account_id, service_id, month_id, year_id) DO NOTHING
        """, cost_data)
        conn.commit()

        logger.info(f"Cost data for account '{account}' stored successfully.")

    except Exception as e:
        logger.error(f"Error processing account '{account}': {e}")

# Main logic
if __name__ == "__main__":
    current_date = datetime.today()
    first_day_of_current_month = current_date.replace(day=1)
    first_day_of_previous_month = (first_day_of_current_month - timedelta(days=1)).replace(day=1)
    start_date = first_day_of_previous_month.strftime("%Y-%m-%d")
    end_date = first_day_of_current_month.strftime("%Y-%m-%d")

    # List of AWS CLI profile names (replace with your actual names)
    aws_accounts = ["account-1"]

    logger.info(f"Starting the script for {len(aws_accounts)} accounts.")
    with ThreadPoolExecutor() as executor:
        executor.map(process_account, aws_accounts)

    # Close the cursor and connection
    cursor.close()
    conn.close()

    logger.info("Script execution completed.")
