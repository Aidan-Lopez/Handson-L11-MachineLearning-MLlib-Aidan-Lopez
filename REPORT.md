# Assignment #4: Report

**Name:**
**Student ID:**
**Email:**

---

## Seed and commands

Seed used for `datagen.py`:

The commands you ran, in order:

```bash

docker compose up -d
python3 datagen.py 801240291
docker cp main.py spark-master:/opt/spark/work-dir/


```

---

## Part 1a: trips and revenue per zone per window

The micro-batch summary (batches, watermark, state rows):

```

=== part1a: micro-batch summary ===
batch input rows            watermark state rows
    0         41                    -         17
    1         37  2026-10-27 07:57:52         27
    2         35  2026-10-27 07:58:42         30
    3         40  2026-10-27 07:59:50         30
    4         38  2026-10-27 08:00:48         24
    5         45  2026-10-27 08:01:48         41
    6         42  2026-10-27 08:02:45         49
    7         39  2026-10-27 08:03:49         49
    8         41  2026-10-27 08:04:50         49
    9         41  2026-10-27 08:05:50         25
   10         44  2026-10-27 08:06:51         42
   11         40  2026-10-27 08:07:57         46
   12         36  2026-10-27 08:08:48         48
   13         42  2026-10-27 08:09:53         49
   14         35  2026-10-27 08:10:48         24
   15         38  2026-10-27 08:11:56         38
   16         45  2026-10-27 08:12:37         47
   17         40  2026-10-27 08:13:49         48
   18         42  2026-10-27 08:14:52         48
   19         43  2026-10-27 08:15:51         25
   20         35  2026-10-27 08:16:45         40
   21         40  2026-10-27 08:17:58         46
   22         40  2026-10-27 08:18:57         47
   23         36  2026-10-27 08:19:44         48
   24         44  2026-10-27 08:20:52         23
   25         44  2026-10-27 08:21:56         38
   26         40  2026-10-27 08:22:55         43
   27         41  2026-10-27 08:23:46         47
   28         37  2026-10-27 08:24:58         47
   29         45  2026-10-27 08:25:52         25
   30          0  2026-10-27 08:26:50         25

```

Which 5-minute windows did the stream write, and which are missing compared with the batch
table printed after the query? For one window that was written, compare its trips with the
batch value: where did the difference come from (look at how `datagen.py` makes the late
events)? Why does `state rows` drop every five batches?


The 5 minute window was 07:55–08:25. The 08:25–08:30 window was missing since the watermark had not been passed yet

Zone 5(08:05) had 25 streaming trips againse the 26 batch trips.

completed windows expire and then state row decreases

## Part 1b: alerts

How many alerts, of which kind, and the first five lines of the output:

```
=== part1b: 12 alerts ===
+-------------+-----+
|       reason|count|
+-------------+-----+
|zero_distance|   12|
+-------------+-----+

```

This query has no watermark and no state. Why is that correct here, and what would change if
you also wanted "more than three alerts for the same driver in ten minutes"?

A watermark was not needed for the independent alerts and counting alerts for every driver would require a state or windowing.


---

## Part 2: the fare model

The three lines printed by `evaluate()` (also in `shared-folder/output/part2/metrics`), the
"tuned" line with the best parameters, and the "best model" line:

```
full       RMSE   1.68 R2  0.969
tuned      RMSE   1.68 R2  0.969
Saving best model: tuned

```

Your tuned model: which estimator, which parameters in the grid and why those, how many fits
the CrossValidator ran, and how long part 2 took. Did the tuning beat the full linear
pipeline? If not, what does that tell you about the data?

I used linear regression with 0.01 and 0.1 of regularization values for testing. The training took about 30 seconds and tuning did not really help much.


Why is `hour` turned into a string before `StringIndexer` (see `features_of`) instead of
being used as a number?

The hour column is categorical so it is a string 

---

## Part 3: scoring the stream

The threshold used, the number of rides flagged, the ten rides with the largest deviation
and the per-window counts:

```

=== part3: micro-batch summary ===
batch input rows            watermark state rows
    0         41                    -          -
    1         37                    -          -
    2         35                    -          -
    3         40                    -          -
    4         38                    -          -
    5         45                    -          -
    6         42                    -          -
    7         39                    -          -
    8         41                    -          -
    9         41                    -          -
   10         44                    -          -
   11         40                    -          -
   12         36                    -          -
   13         42                    -          -
   14         35                    -          -
   15         38                    -          -
   16         45                    -          -
   17         40                    -          -
   18         42                    -          -
   19         43                    -          -
   20         35                    -          -
   21         40                    -          -
   22         40                    -          -
   23         36                    -          -
   24         44                    -          -
   25         44                    -          -
   26         40                    -          -
   27         41                    -          -
   28         37                    -          -
   29         45                    -          -


=== part3: 61 rides flagged ===
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|ride_id|event_time         |pickup_zone|distance_km|duration_min|payment_type|fare  |prediction|deviation|
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
|R00018 |2026-10-27 08:00:05|5          |8.3        |30.0        |app         |132.29|39.8      |92.49    |
|R00584 |2026-10-27 08:14:43|24         |9.35       |38.5        |card        |112.43|35.72     |76.71    |
|R00656 |2026-10-27 08:16:27|5          |4.88       |18.6        |card        |85.14 |29.95     |55.19    |
|R00441 |2026-10-27 08:10:10|14         |7.53       |20.5        |card        |77.17 |26.1      |51.07    |
|R00095 |2026-10-27 08:01:46|1          |5.04       |14.6        |app         |65.7  |22.27     |43.43    |
|R00614 |2026-10-27 08:15:04|2          |4.77       |18.0        |card        |57.9  |20.65     |37.25    |
|R01109 |2026-10-27 08:27:37|5          |3.68       |11.0        |card        |58.9  |25.18     |33.72    |
|R01006 |2026-10-27 08:24:33|9          |14.27      |39.5        |app         |14.94 |44.27     |29.33    |
|R00583 |2026-10-27 08:14:06|1          |1.85       |8.9         |card        |44.14 |14.89     |29.25    |
|R00421 |2026-10-27 08:10:40|3          |13.48      |37.2        |app         |13.07 |42.08     |29.01    |
+-------+-------------------+-----------+-----------+------------+------------+------+----------+---------+
only showing top 10 rows

=== part3: flagged rides per 5-minute window ===
+-------------------+-----+
|       window_start|count|
+-------------------+-----+
|2026-10-27 08:00:00|    7|
|2026-10-27 08:05:00|   10|
|2026-10-27 08:10:00|   14|
|2026-10-27 08:15:00|   13|
|2026-10-27 08:20:00|   11|
|2026-10-27 08:25:00|    6|
+-------------------+-----+

```

Run `python3 datagen.py <seed> shared-folder /tmp/truth.csv` (the third argument writes the
list of planted anomalies, which the generator does not reveal otherwise) and compare it with
your flagged rides: how many of the planted anomalies did you catch, and how many flagged
rides were not planted? Look at the false positives: what do they have in common, and what
would you change in part 2 to reduce them?



Why is there no `fit()` in part 3, and what would happen if you tried one on the stream?
Which stages of the loaded model are fitted models, and what did each one learn from the
training data?

There was no Fit() because the model was trained in part 2. StringIndexer learned the categories, OneHotEncoder learned the sizes of the categories, while Linear Regression learned coeficients.

---

## Design: this pipeline in production

One paragraph. The platform runs this for real: thousands of rides a minute from Kafka, a
model retrained every night on the trips of the last month, anomalies sent to a fraud team.
Where do the training job, the scoring job, the checkpoint and the model live, how does a
new model reach the running scoring job without losing events, and what would you monitor
to know that the model has gone stale?


Kafka receives the rides, and spark scores them. A batch job at night retains from a months trips before. The models are stored on-prem or in cloud. A scoring job restarts from where it left off when a new model is free. Any alerts would probably go to the alert team and I would probable monitor prediction errors.




---

## Problems and fixes

What went wrong and how you fixed it, with the actual error messages. If nothing went wrong,
say so.

everything when smooth
