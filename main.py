"""Hands-on L11: Predicting Airbnb Prices with MLlib Pipelines.

Usage (on the Docker cluster from Hands-on L5):
    spark-submit main.py <task> <shared directory> <seed>

    <task>              task1 or task2
    <shared directory>  the folder mounted in the containers, /opt/spark/work-dir/shared;
                        data is read from <shared>/data, results are written to
                        <shared>/output/<task>, models to <shared>/models
    <seed>              your student ID; it seeds the train/test split (task 1)

task1  splits the data, writes the two sets to Parquet, fits the baseline pipeline of the
       lecture (bedrooms only) and YOUR full pipeline (all features); evaluates both and
       saves the full PipelineModel
task2  loads the saved PipelineModel and scores the test listings, as a scoring job would

Fill in the parts marked TODO. Each task is a function; a task that still returns None is
reported as not implemented, so the file always runs.
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round as sround
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

if len(sys.argv) != 4 or sys.argv[1] not in ("task1", "task2"):
    print(__doc__)
    sys.exit(2)
task, shared, seed = sys.argv[1], sys.argv[2].rstrip("/"), int(sys.argv[3])
data_dir, out_dir, model_dir = f"{shared}/data", f"{shared}/output/{task}", f"{shared}/models/full"

spark = (SparkSession.builder.appName(f"AirbnbML-{task}")
         .config("spark.sql.shuffle.partitions", 4).getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# The evaluators used throughout: RMSE and R2 on the price in dollars.
rmse_eval = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
r2_eval = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")


def evaluate(name, predDF):
    """Print and return (name, rmse, r2) for a DataFrame with price and prediction columns."""
    rmse, r2 = rmse_eval.evaluate(predDF), r2_eval.evaluate(predDF)
    print(f"{name:32s} RMSE {rmse:7.1f}   R2 {r2:6.3f}")
    return (name, round(rmse, 1), round(r2, 3))


def save_metrics(rows, name="metrics"):
    """Write a list of (model, rmse, r2) as one CSV file with a header under out_dir."""
    df = spark.createDataFrame(rows, ["model", "rmse", "r2"])
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{out_dir}/{name}")


def feature_columns(df):
    """The categorical and numeric columns of the dataset, from its schema (slide 23)."""
    categorical = [f for (f, dt) in df.dtypes if dt == "string"]
    numeric = [f for (f, dt) in df.dtypes if dt == "double" and f != "price"]
    return categorical, numeric


def load_split():
    """The training and test sets written by task 1."""
    return spark.read.parquet(f"{data_dir}/split/train"), spark.read.parquet(f"{data_dir}/split/test")


# ---------------------------------------------------------------- Task 1: baseline and full pipeline
def task1_pipelines():
    """Split the data with YOUR seed, write both sets to Parquet under data/split/{train,test},
    then fit and evaluate two pipelines on the test set:

      baseline  VectorAssembler(bedrooms) -> LinearRegression            (given, as in the slides)
      full      StringIndexer (all string columns, handleInvalid="skip") -> OneHotEncoder
                -> VectorAssembler(one-hot columns + numeric columns) -> LinearRegression

    Save the fitted full PipelineModel under model_dir with write().overwrite().save().
    Return the list of (name, rmse, r2) tuples from evaluate(); main writes them to
    output/task1/metrics. Name the models "baseline" and "full".
    """
    airbnbDF = spark.read.parquet(f"{data_dir}/sf-airbnb-clean.parquet")
    trainDF, testDF = airbnbDF.randomSplit([.8, .2], seed=seed)
    trainDF.write.mode("overwrite").parquet(f"{data_dir}/split/train")
    testDF.write.mode("overwrite").parquet(f"{data_dir}/split/test")
    trainDF, testDF = load_split()
    print(f"seed {seed}: {trainDF.count()} training rows, {testDF.count()} test rows")

    # baseline, exactly as in the lecture
    vecAssembler = VectorAssembler(inputCols=["bedrooms"], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="price")
    baseline = Pipeline(stages=[vecAssembler, lr]).fit(trainDF)
    results = [evaluate("baseline", baseline.transform(testDF))]

    # full pipeline
    categorical, numeric = feature_columns(trainDF)
    # TODO: indexer, encoder, assembler, LinearRegression; fit the Pipeline on trainDF;
    #       results.append(evaluate("full", <predictions on testDF>)); save the model
    return None


# ---------------------------------------------------------------- Task 2: score with the saved model
def task2_score():
    """A scoring job: no training data, no fit(). Load the PipelineModel from model_dir,
    transform the test listings (data/split/test), and write neighbourhood_cleansed,
    room_type, bedrooms, price, prediction (rounded to 1 decimal) as one CSV file under
    output/task2/predictions. Print the stage class names of the loaded model and the
    RMSE of its predictions. Return the DataFrame you wrote.
    """
    # TODO
    return None


# ---------------------------------------------------------------- main
if task == "task1":
    result = task1_pipelines()
    if result: save_metrics(result)
else:
    result = task2_score()
if result is None:
    print(f"\n=== {task}: not implemented yet ===")
else:
    print(f"\n=== {task} done; results under {out_dir} ===")
spark.stop()
