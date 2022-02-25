import pyspark
import sys

def hash_funcn(entry):
	# Get datetime and user id.
	datetime, user_id = entry.split(',')

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
user_clicks = sc.textFile(input_uri).filter(lambda x: x != "Datetime,User ID").map(hash_funcn).reduceByKey(lambda count1, count2: count1 + count2)
user_clicks.saveAsTextFile(output_uri)
