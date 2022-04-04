## Setup.
# Imports.
import pyspark
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
import pyspark.sql.functions as f

## Spark app.
# Spark context.
sc = pyspark.SparkContext()

# Spark session.
spark = SparkSession(sc)

## Get data.
# Read stream.
df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10.128.15.205:9092").option("subscribe", "lab-7").load()

# Format.
split_cols = f.split(f.decode(df.value, "UTF-8"), ',')

df = df.withColumn("sepal_length", split_cols.getItem(0))
df = df.withColumn("sepal_width", split_cols.getItem(1))
df = df.withColumn("petal_length", split_cols.getItem(2))
df = df.withColumn("petal_width", split_cols.getItem(3))
df = df.withColumn("class", split_cols.getItem(4))

df.na.drop('any')

for col in ["sepal_length", "sepal_width", "petal_length", "petal_width"]:
    df = df.withColumn(col, df[col].cast("float"))

## Main.
# Model.
model = PipelineModel.load("gs://me18b169_bdl_2022_bucket/model")

# Predictions.
preds = model.transform(df)

# Output.
output_df = preds.withColumn("correct", f.when(f.col("prediction") == f.col("label"), 1).otherwise(0))

df_acc = output_df.select(f.format_number(f.avg("correct")*100, 2).alias("accuracy"))

output_df2 = output_df[["class", "prediction", "label", "correct"]]

query1 = output_df2.writeStream.outputMode("append").format("console").start()

query2 = df_acc.writeStream.outputMode("complete").format("console").start()

query1.awaitTermination()

query2.awaitTermination()

query1.stop()

query2.stop()
