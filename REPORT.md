# Hands-on L11: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used (your student ID):

The commands you ran, in order. If you deviated from the steps in the README, say where and
why.

```bash

```

---

## Results

### Task 1: baseline and full pipeline

Paste the two lines printed by `evaluate()` (also in `shared-folder/output/task1/metrics`),
the five predictions shown and the "saved to" line.

```

```

How many training and test rows did your seed give? The slides show 5,780 and 1,366 with
seed 42; why is yours different?

### Task 2: scoring with the saved model

Paste the stage list, the RMSE of the loaded model and the five predictions.

```

```

---

## Questions

**1. Baseline versus full.** With seed 42 the lecture gets 221.6 / 0.152 for the baseline
and 220.6 / 0.160 for the full pipeline. What did you get, and how much did 97 more
features buy? The price goes from 10 to 10,000 dollars a night (the `summary()` in the
lecture): how does that affect an RMSE measured in dollars, and why do the numbers change
from seed to seed? Name one change to the pipeline you would try first (slides "Running
Example" with the model table, and "Tuning").



**2. What is in `shared-folder/models/full`?** List its folders (`find shared-folder/models/full
-maxdepth 2`) and say what each holds. Why do we save the PipelineModel rather than the
LinearRegressionModel alone? Which stages of the saved model are fitted models and which
are plain transformers?



**3. Only transformers at scoring time.** Task 2 ran no `fit()`. Compare the number of
predictions written with the number of test rows: if they differ, where did the rows go,
and which parameter of which stage decides it? What would you have to change in task 2 to
score a stream of listings instead of a Parquet file (slide "Scoring a Stream")?



---

## Problems and fixes

Anything that went wrong and what resolved it. Paste the actual error message. If nothing
went wrong, say so.
