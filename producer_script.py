import os
import pandas as pd   # pip install pandas

from google.cloud import pubsub_v1    # pip install google-cloud-pubsub
import time
import json
import io
import glob

# Search the current directory for the JSON file (service account key)
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0]

# TODO : fill project id
project_id = "project-8a893975-b0cb-4ab9-992"
topic_id = "smartmeter_raw"

publisher = pubsub_v1.PublisherClient()

# Create topic path
topic_path = publisher.topic_path(project_id, topic_id)

# Read Smart Meter CSV (make sure file exists in same folder)
df = pd.read_csv('smartmeter.csv')

for row in df.iterrows():
    value = row[1].to_dict()

    # Publish as JSON
    future = publisher.publish(
        topic_path,
        json.dumps(value).encode('utf-8')
    )

    print("Measurement with ID " + str(value["id"]) + " is sent")

    time.sleep(0.5)
