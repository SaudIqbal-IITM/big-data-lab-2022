def count_lines_gcf(data, context):
	"""
		Background Cloud Function to be triggered by Cloud Storage.
		This generic function logs relevant data when a file is changed.
		Args:
			data (dict): The Cloud Functions event payload.
			context (google.cloud.functions.Context): Metadata of triggering event.
		Returns:
			None; the output is written to Stackdriver Logging
	"""

	import apache_beam as beam
	from apache_beam.io import ReadFromText
	from apache_beam.io import WriteToText
	from apache_beam.options.pipeline_options import PipelineOptions
	from apache_beam.options.pipeline_options import GoogleCloudOptions
	from apache_beam.options.pipeline_options import StandardOptions

	file = data["name"]

	options = PipelineOptions()

	google_cloud_options = options.view_as(GoogleCloudOptions)
	google_cloud_options.project = "bdl-2022-341111"
	google_cloud_options.job_name = "lab-3-bonus"
	google_cloud_options.temp_location = "gs://me18b169_bdl_2022_bucket/tmp/lab_3/bonus"
	google_cloud_options.region = "us-central1"
	options.view_as(StandardOptions).runner = "DataflowRunner"

	with beam.Pipeline(options=options) as p:
		input = p | "Read Lines"  >> beam.io.ReadFromText(f"gs://me18b169_bdl_2022_bucket/{file}")
		count = input | "Count Lines" >> beam.combiners.Count.Globally()
		output = count | "Write Num Lines"  >> beam.io.WriteToText("gs://me18b169_bdl_2022_bucket/outputs/lab_3/bonus/n_lines.txt")
