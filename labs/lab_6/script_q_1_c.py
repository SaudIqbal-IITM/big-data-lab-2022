## Setup.
# Imports.
from google.cloud import pubsub_v1
from google.cloud import storage

# Client (GCS).
client = storage.Client()

# Bucket.
bucket = client.get_bucket("me18b169_bdl_2022_bucket")

# Client (Pub/Sub subscriber).
subscriber = pubsub_v1.SubscriberClient()

# Topic name.
topic_name = "projects/bdl-2022-341111/topics/lab-6"

# Subscription name.
subscription_name = "projects/bdl-2022-341111/subscriptions/lab-6-sub"

## Logic.
# Callback.
def callback(message):
	filename = message.data.decode('utf-8')

	blob = bucket.get_blob(filename)

	data = blob.download_as_string().decode('utf-8')

	num_lines = len(data.split('\n'))

	print("Number of lines in the file, " + filename + ": " + str(num_lines))

	message.ack()

# Subscribe.
future = subscriber.subscribe(subscription_name, callback)

# Wait.
try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
