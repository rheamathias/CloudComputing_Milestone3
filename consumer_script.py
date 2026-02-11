import os
from google.cloud import pubsub_v1
import glob

# Search the current directory for the JSON file (service account key)
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

project_id = "project-8a893975-b0cb-4ab9-992"
subscription_id = "smartmeter_processed-sub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

import json

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    print(f"Received {json.loads(message.data)}.")
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    streaming_pull_future.result()
