import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler, StringIndexer, MinMaxScaler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import NaiveBayes, LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, MultilayerPerceptronClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


def format_df(data):
    '''
        This function typecast columns to relevant data type.

        Args:
            data (pyspark.sql.dataframe.DataFrame): A dataframe with iris datatset scheme.
        Returns:
            pyspark.sql.dataframe.DataFrame
    '''

    return data \
    .withColumn("sepal_length", col("sepal_length").cast("float")) \
    .withColumn("sepal_width", col("sepal_width").cast("float")) \
    .withColumn("petal_length", col("petal_length").cast("float")) \
    .withColumn("petal_width", col("petal_width").cast("float")) \
    .withColumn("petal_width", col("sepal_length").cast("integer"))


def remove_outliers(data, cols, Q1, Q3, IQR):
    '''
        This function remove outliers from the given dataframe.

        Args:
            data (pyspark.sql.dataframe.DataFrame): A dataframe with iris datatset scheme.
            cols: Column names.
            Q1: 25% quantiles.
            Q3: 50% quantiles.
            IQR: Interquartile range.
        Returns:
            data (pyspark.sql.dataframe.DataFrame)
    '''

    for i, col in zip(range(len(cols)), cols):
        data = data.filter(
            (data[col] >= Q1[i][0] - 1.5*IQR[i][0])
            & (data[col] <= Q3[i][0] + 1.5*IQR[i][0])
        )

    return data

## Spark app.
# Spark context.
sc = pyspark.SparkContext()

# Spark session.
spark = SparkSession(sc)

## Get data.
# Read data from the table bdl-2022-341111.iris_dataset.iris_plants_data.
iris_plants_data = (
    spark.read.format("bigquery")
    .option("table", "bdl-2022-341111.iris_dataset.iris_plants_data")
    .load()
)

# Create a temporary view.
iris_plants_data.createOrReplaceTempView("iris")

# Remove rows with null values.
data = spark.sql(
    """
    SELECT *
    FROM iris
    WHERE 
      sepal_length IS NOT NULL
      AND sepal_width IS NOT NULL
      AND petal_length IS NOT NULL
      AND petal_width IS NOT NULL
      AND class IS NOT NULL
  """
)

## Feature and target variables.
# Features.
feature_cols = ["sepal_length", "sepal_width", "petal_length", "petal_width"]

# Target.
target_col = "label"

## Preprocessing.
# Encode class to integer values.
string_indexer = StringIndexer(inputCol="class", outputCol=target_col)
string_indexer = string_indexer.fit(data)

data = string_indexer.transform(data)

# Drop 'class' column.
data = data.drop("class")

# Split the dataset into train and test datasets.
training_data, test_data = data.randomSplit([0.8, 0.2], seed=0)

# Typecast columns to relevant data types.
training_data, test_data = format_df(training_data), format_df(test_data)

# Remove outliers.
Q1 = training_data.approxQuantile(feature_cols, [0.25], 0.5)
Q3 = training_data.approxQuantile(feature_cols, [0.75], 0.5)

IQR = []
for q1, q3 in zip(Q1, Q3):
    IQR.append([q3[0]-q1[0]])

training_data, test_data = remove_outliers(training_data, feature_cols, Q1, Q3, IQR), remove_outliers(test_data, feature_cols, Q1, Q3, IQR)

# Vectorize and scale the features.
preprocess_pipeline = Pipeline(
    stages=[
        VectorAssembler(inputCols=feature_cols, outputCol="features"), 
        MinMaxScaler(inputCol="features", outputCol="scaled_features")
    ]
)

preprocess_pipeline = preprocess_pipeline.fit(training_data)

training_data = preprocess_pipeline.transform(training_data)
test_data = preprocess_pipeline.transform(test_data)

# Drop irrelevant columns.
for col in feature_cols + ["features"]:
    training_data = training_data.drop(col)
    test_data = test_data.drop(col)

# Rename 'scaled_features' column to 'features'.
training_data = training_data.withColumnRenamed("scaled_features", "features")
test_data = test_data.withColumnRenamed("scaled_features", "features")

## Modelling.
# Model evaluator.
evaluator = MulticlassClassificationEvaluator(
    predictionCol="prediction",
    labelCol="label",
    metricName="accuracy"
)

# Naive Bayes.
nb = NaiveBayes(modelType="multinomial")
nb_params_grid = ParamGridBuilder().addGrid(nb.smoothing, [0.01, 0.1, 1, 10]).build()

# Logistic regression.
lr = LogisticRegression(family="multinomial")
lr_params_grid = ParamGridBuilder().addGrid(lr.regParam, [0.01, 0.1, 1, 10]).addGrid(lr.elasticNetParam, [0.01, 0.1, 1]).build()

# Decision trees.
dt = DecisionTreeClassifier()
dt_params_grid = ParamGridBuilder().addGrid(dt.impurity,['gini', 'entropy']).addGrid(dt.maxDepth, [1, 5, 10]).build()

# Random forest.
rf = RandomForestClassifier()
rf_params_grid = ParamGridBuilder().addGrid(rf.numTrees, [1, 5, 10]).addGrid(rf.maxDepth, [1, 5, 10]).addGrid(rf.impurity, ['gini', 'entropy']).build()

# Multi layer perceptron.
mp = MultilayerPerceptronClassifier()
mp_params_grid = ParamGridBuilder().addGrid(mp.layers, [[4, 5, 4, 3], [4, 5, 6, 5, 4, 3]]).build()

classifiers = {
    "Naive Bayes": {
        "model": nb,
        "params_grid": nb_params_grid
    },
    "Logistic Regression": {
        "model": lr,
        "params_grid": lr_params_grid
    },
    "Decision Tree Classifier": {
        "model": dt,
        "params_grid": dt_params_grid
    },
    "Random Forest Classifier": {
        "model": rf,
        "params_grid": rf_params_grid
    },
    "Multilayer Perceptron Classifier": {
        "model": mp,
        "params_grid": mp_params_grid
    }
}

# Train and evaluate different models.
for classifier_name, classifier in classifiers.items():
    model = classifier["model"]
    params_grid = classifier["params_grid"]

    cv = CrossValidator(
        estimator=model,
        estimatorParamMaps=params_grid,
        evaluator=evaluator,
        numFolds=6
    )

    cv = cv.fit(training_data)

    training_pred = cv.transform(training_data)
    test_pred = cv.transform(test_data)

    print("="*50)
    print(f"{classifier_name} Training Accuracy: {evaluator.evaluate(training_pred)} Test Accuracy: {evaluator.evaluate(test_pred)}")
    print("="*50)
