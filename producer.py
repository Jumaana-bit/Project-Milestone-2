from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob
import json
import os
import csv

# Set up Google Cloud Pub/Sub credentials
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# Configuration
project_id = "bold-gearbox-448618-f9"
topic_name = "records"

# Create a publisher client and topic path
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing messages to {topic_path}...\n")

# Read CSV and publish messages
csv_file = "Labels.csv"  # Replace with your CSV file path

try:
    with open(csv_file, mode="r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            # Serialize the dictionary to JSON and publish
            message = json.dumps(row).encode("utf-8")  # Serialization
            print(f"Producing record: {row}")
            future = publisher.publish(topic_path, message)
            future.result()  # Ensure publishing succeeded
        print("All messages published.")
except Exception as e:
    print(f"Error reading or publishing messages: {e}")
