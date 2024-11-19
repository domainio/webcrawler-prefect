"""Main ETL workflow module."""
import time
from prefect import flow, task
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@task
def fetch_data():
    """Fetch sample data with a delay."""
    print("Fetching data...")
    time.sleep(10)
    return {"data": "sample"}

@task
def process_data(input_data):
    """Process the input data with a delay."""
    print("Processing data:", input_data)
    time.sleep(5)
    return "processed " + str(input_data)

@flow
def my_etl_flow():
    """Main ETL workflow."""
    data = fetch_data()
    result = process_data(data)
    print("Flow completed with result:", result)
    return result

if __name__ == "__main__":
    my_etl_flow()
