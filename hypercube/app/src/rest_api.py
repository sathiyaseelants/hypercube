import requests

def to_api(df):
    # REST call to PUT the data to an API endpoint
    url = "https://hypercube.ingestion.com/wind-orders"
    # Converting df to JSON format for API call
    data = df.toJSON().collect()
    response = requests.put(url, json=data)
    if response.status_code == 200:
        print("Data persisted to API successfully.")
    else:
        print(f"Failed to persist data to API. Status code: {response.status_code}")
