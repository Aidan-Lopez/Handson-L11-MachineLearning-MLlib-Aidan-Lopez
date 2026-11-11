
"""Assignment #4: Ride-Sharing Analytics, from Stream to Model and Back.

Usage:
    spark-submit main.py <part> <shared directory> <seed>
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as ssum, avg, round as sround,
    window, hour, abs as sabs, lit
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder
)
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator


PARTS = ("part1a", "part1b", "part2", "part3")

if len(sys.argv) != 4 or sys.argv[1] not in PARTS:
    print(__doc__)
    sys.exit(2)

part, shared, seed = sys.argv[1], sys.argv[2].rstrip("/"), int(sys.argv[3])

in_dir = f"{shared}/input"
stream_dir = f"{shared}/stream"
out_dir = f"{shared}/output/{part}"
ckpt_dir = f"{shared}/checkpoint/{part}"
model_dir = f"{shared}/models/fare"

spark = (
    SparkSession.builder
    .appName(f"A4-{part}")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ride_schema = (
    "ride_id STRING, driver_id STRING, pickup_zone INT, "
    "distance_km DOUBLE, duration_min DOUBLE, "
    "payment_type STRING, fare DOUBLE, event_time TIMESTAMP"
)

trip_schema = (
    "trip_id STRING, driver_id STRING, pickup_zone INT, "
    "dropoff_zone INT, pickup_ts TIMESTAMP, "
    "distance_km DOUBLE, duration_min DOUBLE, "
    "payment_type STRING, fare DOUBLE"
)


def read_rides():
    return (
        spark.readStream
        .schema(ride_schema)
        .option("maxFilesPerTrigger", 1)
        .json(stream_dir)
    )


def run(query, label):
    query.awaitTermination()

    print(f"\n=== {label}: micro-batch summary ===")
    print(
        f"{'batch':>5} {'input rows':>10} "
        f"{'watermark':>20} {'state rows':>10}"
    )

    for p in query.recentProgress:
        wm = p.get("eventTime", {}).get("watermark", "-")[:19]
        wm = wm.replace("T", " ")
        wm = "-" if wm.startswith("1970") else wm

        ops = p.get("stateOperators", [])

        print(
            f"{p['batchId']:>5} "
            f"{p['numInputRows']:>10} "
            f"{wm:>20} "
            f"{ops[0]['numRowsTotal'] if ops else '-':>10}"
        )


# -------------------------------------------------------
# Part 1a: Revenue per zone per window
# -------------------------------------------------------

def part1a_zone_windows():

    rides = read_rides()

    windows = (
        rides
        .withWatermark("event_time", "3 minutes")
        .groupBy(
            window(col("event_time"), "5 minutes"),
            "pickup_zone"
        )
        .agg(
            count("*").alias("trips"),
            sround(ssum("fare"), 2).alias("revenue")
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "pickup_zone",
            "trips",
            "revenue"
        )
    )

    return (
        windows.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", out_dir)
        .option("checkpointLocation", ckpt_dir)
        .trigger(availableNow=True)
        .start()
    )


# -------------------------------------------------------
# Part 1b: Alerts
# -------------------------------------------------------

def part1b_alerts():

    from pyspark.sql.functions import when

    rides = read_rides()

    alerts = (
        rides
        .filter(
            (col("fare") > 150) |
            (col("distance_km") == 0)
        )
        .withColumn(
            "reason",
            when(col("fare") > 150, "high_fare")
            .otherwise("zero_distance")
        )
        .select(
            "ride_id",
            "event_time",
            "pickup_zone",
            "distance_km",
            "duration_min",
            "fare",
            "reason"
        )
    )

    return (
        alerts.writeStream
        .format("csv")
        .outputMode("append")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("path", out_dir)
        .option("checkpointLocation", ckpt_dir)
        .trigger(availableNow=True)
        .start()
    )


# -------------------------------------------------------
# Part 2: Train fare model
# -------------------------------------------------------

def features_of(df, ts_col):
    return (
        df.withColumn("hour", hour(col(ts_col)).cast("string"))
          .withColumn("zone", col("pickup_zone").cast("string"))
    )


def part2_train():

    rmse_eval = RegressionEvaluator(
        labelCol="fare",
        predictionCol="prediction",
        metricName="rmse"
    )

    r2_eval = RegressionEvaluator(
        labelCol="fare",
        predictionCol="prediction",
        metricName="r2"
    )

    trips = spark.read.csv(
        f"{in_dir}/trips_history.csv",
        header=True,
        schema=trip_schema
    )

    trips = features_of(trips, "pickup_ts")

    trainDF, testDF = trips.randomSplit(
        [0.8, 0.2],
        seed=seed
    )

    trainDF.cache()

    print(
        f"seed {seed}: "
        f"{trainDF.count()} training rows, "
        f"{testDF.count()} test rows"
    )

    results = []
    models = {}
    actual_rmse = {}

    def evaluate(name, model):

        pred = model.transform(testDF)

        rmse = rmse_eval.evaluate(pred)
        r2 = r2_eval.evaluate(pred)

        print(
            f"{name:10s} "
            f"RMSE {rmse:6.2f} "
            f"R2 {r2:6.3f}"
        )

        results.append((
            name,
            round(rmse, 2),
            round(r2, 3)
        ))

        models[name] = model
        actual_rmse[name] = rmse

    # Baseline: Distance only

    baseline = Pipeline(stages=[
        VectorAssembler(
            inputCols=["distance_km"],
            outputCol="baseline_features"
        ),
        LinearRegression(
            featuresCol="baseline_features",
            labelCol="fare"
        )
    ])

    baseline_model = baseline.fit(trainDF)

    evaluate("baseline", baseline_model)

    # Full model: Categorical + numerical features

    def full_stages():

        categories = [
            "zone",
            "payment_type",
            "hour"
        ]

        indexed = [
            name + "_idx"
            for name in categories
        ]

        encoded = [
            name + "_ohe"
            for name in categories
        ]

        indexers = [
            StringIndexer(
                inputCol=name,
                outputCol=idx,
                handleInvalid="keep"
            )
            for name, idx in zip(categories, indexed)
        ]

        encoder = OneHotEncoder(
            inputCols=indexed,
            outputCols=encoded
        )

        assembler = VectorAssembler(
            inputCols=encoded + [
                "distance_km",
                "duration_min"
            ],
            outputCol="full_features"
        )

        return indexers + [encoder, assembler]

    full = Pipeline(
        stages=full_stages() + [
            LinearRegression(
                featuresCol="full_features",
                labelCol="fare"
            )
        ]
    )

    full_model = full.fit(trainDF)

    evaluate("full", full_model)

    # Tuned model with cross-validation

    tuned_lr = LinearRegression(
        featuresCol="full_features",
        labelCol="fare",
        elasticNetParam=0.0
    )

    tuned = Pipeline(
        stages=full_stages() + [tuned_lr]
    )

    grid = (
        ParamGridBuilder()
        .addGrid(
            tuned_lr.regParam,
            [0.01, 0.1]
        )
        .build()
    )

    cv = CrossValidator(
        estimator=tuned,
        estimatorParamMaps=grid,
        evaluator=rmse_eval,
        numFolds=3,
        parallelism=2,
        seed=seed
    )

    cv_model = cv.fit(trainDF)

    evaluate("tuned", cv_model.bestModel)

    # Save model with lowest RMSE

    best_name = min(
        actual_rmse,
        key=actual_rmse.get
    )

    print(f"Saving best model: {best_name}")

    models[best_name].write().overwrite().save(model_dir)

    trainDF.unpersist()

    return results


# -------------------------------------------------------
# Part 3: Score streaming rides
# -------------------------------------------------------

def part3_score(threshold):

    model = PipelineModel.load(model_dir)

    rides = features_of(
        read_rides(),
        "event_time"
    )

    scored = model.transform(rides)

    flagged = (
        scored
        .withColumn(
            "deviation",
            sround(
                sabs(
                    col("fare") - col("prediction")
                ),
                2
            )
        )
        .filter(col("deviation") > threshold)
        .select(
            "ride_id",
            "event_time",
            "pickup_zone",
            "distance_km",
            "duration_min",
            "payment_type",
            "fare",
            sround(
                col("prediction"), 2
            ).alias("prediction"),
            "deviation"
        )
    )

    return (
        flagged.writeStream
        .format("csv")
        .outputMode("append")
        .option("header", True)
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .option("path", out_dir)
        .option("checkpointLocation", ckpt_dir)
        .trigger(availableNow=True)
        .start()
    )


# -------------------------------------------------------
# Main
# -------------------------------------------------------

def metrics_of_saved_model():

    m = spark.read.csv(
        f"{shared}/output/part2/metrics",
        header=True,
        inferSchema=True
    )

    best = m.orderBy("rmse").first()

    return best["model"], float(best["rmse"])


if part == "part1a":

    q = part1a_zone_windows()

    if q is not None:

        run(q, part)

        print("\n=== part1a: windows written by the stream ===")

        spark.read.parquet(out_dir) \
            .orderBy("window_start", "pickup_zone") \
            .show(100, truncate=False)

        print("=== part1a: the same aggregation in batch ===")

        (
            spark.read.schema(ride_schema)
            .json(stream_dir)
            .groupBy(
                window(col("event_time"), "5 minutes"),
                "pickup_zone"
            )
            .agg(
                count("*").alias("trips"),
                sround(ssum("fare"), 2).alias("revenue")
            )
            .select(
                col("window.start").alias("window_start"),
                "pickup_zone",
                "trips",
                "revenue"
            )
            .orderBy("window_start", "pickup_zone")
            .show(100, truncate=False)
        )

elif part == "part1b":

    q = part1b_alerts()

    if q is not None:

        run(q, part)

        alerts = spark.read.csv(
            out_dir,
            header=True
        )

        print(f"\n=== part1b: {alerts.count()} alerts ===")

        alerts.groupBy("reason").count().show()

elif part == "part2":

    result = part2_train()

    q = None if result is None else True

    if result:

        (
            spark.createDataFrame(
                result,
                ["model", "rmse", "r2"]
            )
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(f"{out_dir}/metrics")
        )

else:

    name, rmse = metrics_of_saved_model()

    threshold = round(4 * rmse, 2)

    print(
        f"saved model: {name}, "
        f"test RMSE {rmse}; "
        f"flagging rides with deviation > {threshold}"
    )

    q = part3_score(threshold)

    if q is not None:

        run(q, part)

        flagged = spark.read.csv(
            out_dir,
            header=True,
            inferSchema=True
        )

        print(f"\n=== part3: {flagged.count()} rides flagged ===")

        flagged.orderBy(
            col("deviation").desc()
        ).show(10, truncate=False)

        print("=== part3: flagged rides per 5-minute window ===")

        (
            flagged
            .groupBy(
                window(col("event_time"), "5 minutes")
            )
            .count()
            .select(
                col("window.start").alias("window_start"),
                "count"
            )
            .orderBy("window_start")
            .show()
        )


if q is None:
    print(f"\n=== {part}: not implemented yet ===")
else:
    print(f"\n=== {part} done; results under {out_dir} ===")

spark.stop()
