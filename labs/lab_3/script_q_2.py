import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "bdl-2022-341111"
google_cloud_options.job_name = "lab-3-q-2"
google_cloud_options.temp_location = "gs://me18b169_bdl_2022_bucket/tmp/lab_3/q_2"
google_cloud_options.region = "us-central1"
options.view_as(StandardOptions).runner = "DataflowRunner"

with beam.Pipeline(options=options) as p:
	input = p | "Read Lines"  >> beam.io.ReadFromText("gs://bdl2022/lines_big.txt")
	num_words = input | "Number of Words" >> beam.FlatMap(lambda line: [len(line.split())])
	avg_num_words = num_words | "Average Number Words" >> beam.combiners.Mean.Globally()
	output = avg_num_words | "Write Average Number Words"  >> beam.io.WriteToText("gs://me18b169_bdl_2022_bucket/outputs/lab_3/q_2/avg_num_words.txt")
