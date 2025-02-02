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

# Reads CSV file and publishes each row to the csvRecords topic
# Using MySQL connector and application integration this data is written to MySQL database

try:
    with open(csv_file, mode="r") as file:
        csv_reader = csv.DictReader(file)

        for row in csv_reader:
            # Convert CSV values to correct data types
            formatted_row = {
                "Timestamp": int(row["Timestamp"]),
                "Car1_Location_X": float(row["Car1_Location_X"]),
                "Car1_Location_Y": int(row["Car1_Location_Y"]),
                "Car1_Location_Z": float(row["Car1_Location_Z"]),
                "Car2_Location_X": float(row["Car2_Location_X"]),
                "Car2_Location_Y": int(row["Car2_Location_Y"]),
                "Car2_Location_Z": float(row["Car2_Location_Z"]),
                "Occluded_Image_view": row["Occluded_Image_view"],
                "Occluding_Car_view": row["Occluding_Car_view"],
                "Ground_Truth_View": row["Ground_Truth_View"],
                "pedestrianLocationX_TopLeft": int(row["pedestrianLocationX_TopLeft"]),
                "pedestrianLocationY_TopLeft": int(row["pedestrianLocationY_TopLeft"]),
                "pedestrianLocationX_BottomRight": int(row["pedestrianLocationX_BottomRight"]),
                "pedestrianLocationY_BottomRight": int(row["pedestrianLocationY_BottomRight"])
            }

            # Convert row to JSON format
            message_json = json.dumps(formatted_row).encode("utf-8")

            # Publish to Pub/Sub
            future = publisher.publish(topic_path, message_json)
            future.result()  # Ensure successful publishing

            print(f"Published CSV record: {formatted_row}")

    print("All CSV records published successfully.")

except Exception as e:
    print(f"Error publishing CSV records: {e}")