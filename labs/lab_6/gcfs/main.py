def pub_filename(data, context):
	"""
		Background Cloud Function to be triggered by Cloud Storage.
		This generic function logs relevant data when a file is changed.
		Args:
			data (dict): The Cloud Functions event payload.
			context (google.cloud.functions.Context): Metadata of triggering event.
		Returns:
			None; the output is written to Stackdriver Logging
	"""

	## Setup.
	# Imports.
	from google.cloud import pubsub_v1

	# Client (Pub/Sub publisher).
	publisher = pubsub_v1.PublisherClient()

	# Topic name.
	topic_name = "projects/bdl-2022-341111/topics/lab-6"

	## Logic.
	# Filename.
	filename = data["name"]

	# Publish the filename.
	future = publisher.publish(topic_name, bytes(filename, "utf-8"))

	# Wait.
	future.result()

