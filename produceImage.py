from google.cloud import pubsub_v1  # pip install google-cloud-pubsub
import glob
import base64
import os

# Search for the JSON file (Google Cloud credentials)
service_account_files = glob.glob("*.json")
if not service_account_files:
    raise FileNotFoundError("No service account JSON file found in the directory.")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_files[0]

# Google Cloud Pub/Sub settings
project_id = "bold-gearbox-448618-f9"
topic_name = "images"

# Create a publisher client with ordering enabled
publisher_options = pubsub_v1.types.PublisherOptions(enable_message_ordering=True)
publisher = pubsub_v1.PublisherClient(publisher_options=publisher_options)
topic_path = publisher.topic_path(project_id, topic_name)

print(f"Publishing images to topic: {topic_path}")

# Search for image files in the current folder
image_files = glob.glob("*.jpg") + glob.glob("*.png") + glob.glob("*.jpeg")

if not image_files:
    print("No images found in the current directory.")
else:
    for image_path in image_files:
        with open(image_path, "rb") as f:
            image_data = base64.b64encode(f.read()).decode("utf-8")  # Encode and convert to string

        image_name = os.path.basename(image_path)  # Use filename as key

        try:
            future = publisher.publish(topic_path, image_data.encode(), ordering_key=image_name)
            future.result()  # Ensure the message is published successfully
            print(f"Published: {image_name}")
        except Exception as e:
            print(f"Failed to publish {image_name}: {e}")
