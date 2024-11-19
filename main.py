from prefect import flow, task

@task
def fetch_data():
    print("Fetching data...")

@task
def process_data():
    print("Processing data...")

@flow
def my_etl_flow():
    fetch_data()
    process_data()

if __name__ == "__main__":
    my_etl_flow()