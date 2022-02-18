import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


class AverageFn(beam.CombineFn):
  def create_accumulator(self):
    return (0.0, 0)

  def add_input(self, sum_count, input):
    (sum, count) = sum_count
    return sum + len(input.split()), count + 1

  def merge_accumulators(self, accumulators):
    sums, counts = zip(*accumulators)
    return sum(sums), sum(counts)

  def extract_output(self, sum_count):
    (sum, count) = sum_count
    return sum / count if count else float('NaN')


options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = "bdl-2022-341111"
google_cloud_options.job_name = "lab-3-adv"
google_cloud_options.temp_location = "gs://me18b169_bdl_2022_bucket/tmp/lab_3/adv"
google_cloud_options.region = "us-central1"
options.view_as(StandardOptions).runner = "DataflowRunner"

with beam.Pipeline(options=options) as p:
	input = p | "Read Lines"  >> beam.io.ReadFromText("gs://bdl2022/lines_big.txt")
	avg_num_words = input | "Average Number Words" >> beam.CombineGlobally(AverageFn())
	output = avg_num_words | "Write Average Number Words"  >> beam.io.WriteToText("gs://me18b169_bdl_2022_bucket/outputs/lab_3/adv/avg_num_words.txt")
