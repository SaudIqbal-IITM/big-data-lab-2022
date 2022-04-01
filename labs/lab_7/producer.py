## Setup.
# Imports.
from kafka import KafkaProducer
import csv
from google.cloud import storage

# Client (GCS).
client = storage.Client()

# Bucket.
bucket = client.get_bucket("me18b169_bdl_2022_bucket")

# Kafka producer.
producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092'

## Logic.
# Download data file from GCS.
blob = bucket.get_blob("iris.data")
data = blob.download_as_string().decode('utf-8')

# Write data row by row to Kafka topic.
for entry in data.split('\n')[1:]:
	# Send.
	producer.send("lab-7", entry)

	# Wait.
	producer.flush()
