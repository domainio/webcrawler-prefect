import time
from prefect import flow, task
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@task
def fetch_data():
    print("Fetching data...")
    time.sleep(10)
    return {"data": "sample"}

@task
def process_data(input_data):
    print("Processing data:", input_data)
    time.sleep(5)
    return "processed " + str(input_data)

@flow
def my_etl_flow():
    data = fetch_data()
    result = process_data(data)
    print("Flow completed with result:", result)

if __name__ == "__main__":
    my_etl_flow()