import pyspark
import sys

def hash_funcn(entry):
	# Get all the values.
	values = entry.split(',')

	# Ignore the header.
	if values[0] == "Datetime":
		return None

	# Datetime.
	datetime = values[0]

	# User ID.
	user_id = values[1]

	# Segregate date and time.
	date, time = datetime.split(' ')

	# Segregate hour, minute and second.
	hour, minute, second = time.split(':')

	# Typecast hour, minute and second from str to int.
	hour, minute, second = int(hour), int(minute), int(second)

	# Map.
	if hour < 6:
		return ("0-6", 1)
	if hour < 12:
		return ("6-12", 1)
	if hour < 18:
		return ("12-18", 1)
	if hour < 24:
		return ("18-24", 1)

## Check.
# Args check.
if len(sys.argv) != 3:
  raise Exception("Exactly 2 arguments are required: <input-uri> <output-uri>")

# Inputs.
# Input URI.
input_uri = sys.argv[1]

# Output URI.
output_uri = sys.argv[2]

## Spark app.
# Spark context.
sc = pyspark.SparkContext()

# Logic.
counts = sc.textFile(input_uri).flatMap(hash_funcn).reduceByKey(lambda count1, count2: count1 + count2).coalesce(1)
counts.saveAsTextFile(output_uri)
