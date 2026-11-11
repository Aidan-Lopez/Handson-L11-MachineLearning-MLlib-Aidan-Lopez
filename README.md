# Hands-on L11: Predicting Airbnb Prices with MLlib Pipelines

**ITCS 6190/8190 - Cloud Computing for Data Analysis - Fall 2026**

In the lecture we predicted the nightly price of an Airbnb listing from its number of
bedrooms, with a two-stage pipeline, and got an RMSE of 221.6 dollars. This week you build
the real thing on the same data and the same Docker cluster: a pipeline over **all** the
columns, categorical ones included, evaluated against that baseline, and a scoring job
that loads the saved **PipelineModel** and applies it to listings it has never seen. Two
tasks in `main.py`, each one a `spark-submit`.

**Worth 1 point.** Submit the repository link on Canvas by 11:59 pm on the day of the class.
Work individually.

---

## The data

`shared-folder/data/sf-airbnb-clean.parquet` is the dataset of the lecture and of the
Learning Spark book: 7,146 Airbnb listings in San Francisco, 34 columns, already cleaned
(zero prices removed, integers converted to doubles, missing values imputed with an
indicator column). The label is `price`, the nightly price in dollars; 7 columns are
strings (`neighbourhood_cleansed`, `room_type`, `property_type`, ...) and 26 are numeric.

The data is the same for everyone, but the **train/test split is yours**: task 1 splits it
with `randomSplit([.8, .2], seed=<your student ID>)` and writes the two sets to Parquet, so
that task 2 uses exactly the same rows and the grader can reproduce your numbers.

---

## The tasks

```
spark-submit main.py <task> <shared directory> <seed>
```

`main.py` gives you the SparkSession, the evaluators (RMSE and R2 on the price in dollars),
a helper that lists the categorical and numeric columns from the schema, the split, and
the baseline pipeline of the lecture. The docstring of each task function states exactly
what to build.

**Task 1. Baseline and full pipeline.** The baseline (`VectorAssembler(bedrooms)` then
`LinearRegression`) is given. You build the full pipeline of the slides "All Features at
Once": `StringIndexer` over all string columns with `handleInvalid="skip"`, `OneHotEncoder`,
`VectorAssembler` over the one-hot columns plus the numeric columns, `LinearRegression`.
Both are fitted on the training set and evaluated on the test set; the metrics go to
`shared-folder/output/task1/metrics`, and the full PipelineModel is saved under
`shared-folder/models/full`.

**Task 2. A scoring job.** No training data, no `fit()`: load the PipelineModel, transform
the test listings, write `neighbourhood_cleansed, room_type, bedrooms, price, prediction`
to `shared-folder/output/task2/predictions`, print the stage class names and the RMSE.

Everything uses the pipeline API of `pyspark.ml`. No `pyspark.mllib`, no pandas, no
scikit-learn.

---

## What is in this repository

| Path                     | What it is                                                                          |
| ------------------------ | ----------------------------------------------------------------------------------- |
| `docker-compose.yml`     | the cluster from Hands-on L5: one Spark master and two workers, `apache/spark:4.2.0` |
| `docker-compose.codespaces.yml`, `conf/spark-defaults.conf` | the same cluster, for GitHub Codespaces (see step 2) |
| `main.py`                | the two tasks: setup, evaluators and the baseline are given, the rest is yours      |
| `REPORT.md`              | the report template you fill in                                                     |
| `shared-folder/data/`    | the Airbnb Parquet file; task 1 adds `split/train` and `split/test`                 |
| `shared-folder/output/`  | where the results land (`task1/`, `task2/`)                                         |
| `shared-folder/models/`  | where task 1 saves the full PipelineModel (not committed)                           |

`shared-folder/` is mounted into every container at `/opt/spark/work-dir/shared`, so the
data is visible to the master and the workers, and what the workers write appears on your
disk.

---

## Prerequisites

- **Docker Desktop**, running. Nothing else: the data is in the repository.

---

## Steps

### 1. Make your own copy of this repository

On the repository page, click the green **Use this template** button, then
**Create a new repository**. Name it `ITCS6190-H11-<your-name>` and set the visibility to
**Public**. Do not fork and do not clone this repository directly. Clone *your* repository
and work in it.

### 2. Start the cluster

```bash
docker compose up -d
```

Open <http://localhost:8080> and check that the master lists two workers in state ALIVE.

**In a GitHub Codespace**, start the cluster with the other compose file instead, as in the
previous hands-ons:

```bash
docker compose -f docker-compose.codespaces.yml up -d
```

(Inside a Codespace, traffic between containers on a Compose network can be dropped, so the
workers never register with the master. That file puts the whole cluster on one network.
Everything else is the same, except that `docker compose down` in step 6 also needs the
`-f docker-compose.codespaces.yml` flag.)

### 3. Run task 1 as it is

```bash
docker cp main.py spark-master:/opt/spark/work-dir/

docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/work-dir/main.py task1 /opt/spark/work-dir/shared <your student ID>
```

It splits the data with your seed, writes `shared-folder/data/split/`, fits and evaluates
the baseline, and reports `task1: not implemented yet` because the full pipeline is
missing. Write your seed in `REPORT.md` now.

### 4. Fill in the tasks, one at a time

Edit `main.py` on your machine. After each change, repeat the `docker cp` and the
`spark-submit` from step 3 with `task1` or `task2`. Each run takes about a minute on the
cluster.

While a task runs, open the Spark UI at <http://localhost:4040>: each `fit()` shows up as
a group of jobs in the Jobs tab, and `transform()` shows up only when something is written.

Useful commands to look at the outputs on your machine:

```bash
cat shared-folder/output/task1/metrics/*.csv
head -5 shared-folder/output/task2/predictions/*.csv
find shared-folder/models/full -maxdepth 2        # what a saved PipelineModel looks like
```

### 5. Answer the questions

The three questions in `REPORT.md` are about your metrics, the saved model folder, and
what a scoring job may and may not do. Answer with your own numbers.

### 6. Stop the cluster

```bash
docker compose down
```

---

## What to submit

Commit and push:

- `main.py` with the two tasks implemented;
- the outputs under `shared-folder/output/task1` and `task2` (the `part-*.csv`
  files; `.gitignore` leaves out `_SUCCESS` and the `.crc` files);
- `REPORT.md` filled in, with your seed, the results and the answers to the three questions.

Do not commit the split or the model (`.gitignore` takes care of both) and do not modify
the data, the compose files, the evaluators or the baseline.

Submit the link to your repository on Canvas.

---

## Grading (1 point)

| Criterion | Points |
| --------- | ------ |
| Repository created from the template, public, with the required files and nothing else | 0.15 |
| Task results: the metrics of task 1 and the predictions of task 2 are correct for your seed | 0.35 |
| Pipeline done right: StringIndexer with handleInvalid, OneHotEncoder, VectorAssembler and LinearRegression as stages of one Pipeline fitted on the training set only, saved as a PipelineModel; task 2 loads it and runs no fit() | 0.20 |
| Questions 1 to 3 answered with the numbers from your own run (baseline versus full and your split, the saved folder, scoring time) | 0.20 |
| Results pasted and discussed, problems reported with their error message and fix | 0.10 |

---

## Tips

- `StringIndexer` and `OneHotEncoder` accept lists of input and output columns; one of each
  handles all seven string columns. Build the output names from the input names.
- Every stage goes inside the `Pipeline`; never call `fit` or `transform` on a stage by
  hand, or the test set will be transformed with parameters that differ from training.
- `PipelineModel.load` is a class method: `PipelineModel.load(path)`, not
  `PipelineModel().load(path)`.
- If a run fails half way, the output folders of that task may be incomplete; the next
  successful run overwrites them.
- Container names (`spark-master`) and ports are the same as in the previous hands-ons. If
  `docker compose up` complains that a port is in use, stop the other cluster first.
