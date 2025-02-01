import redis        # pip install redis
import io
import base64
import os

# Redis connection details
ip = "35.203.111.228"
password = "sofe4630u"

# Connect to Redis
r = redis.Redis(host=ip, port=6379, db=0, password=password)

# Retrieve all keys stored in Redis
keys = r.keys()  # Get all stored keys

if not keys:
    print("No images found in Redis.")
else:
    save_dir = "./received_images"
    os.makedirs(save_dir, exist_ok=True)  # Create directory if not exists

    for key in keys:
        key = key.decode()  # Convert byte key to string
        value = r.get(key)  # Get the image data

        if value:
            decoded_value = base64.b64decode(value)  # Decode the image

            file_path = os.path.join(save_dir, key)  # Save with original filename
            with open(file_path, "wb") as f:
                f.write(decoded_value)

            print(f"Image '{key}' received and saved to {file_path}")

print("All images received successfully.")

