# Presentation and viva guide

## Suggested 8-minute presentation

**0–1 min: Problem.** Millions of minute readings are difficult to interpret directly. We want reliable energy totals, demand patterns, a tomorrow forecast, and unusual-day flags.

**1–2 min: Dataset.** Explain the original UCI text file, date range, missing markers, and correct power-to-energy conversion. Point to the measured manifest.

**2–3 min: Architecture.** HDFS stores raw and partitioned processed data; YARN allocates resources; Spark computes ETL/SQL; MapReduce independently aggregates the same readings. Modeling works on daily aggregates. Show actual application IDs.

**3–5 min: Dashboard.** Demonstrate date filtering, circuit shares and hourly/weekday rhythm. Mention coverage before interpreting any total.

**5–6 min: Forecast.** Explain calendar lags, past-only rolling statistics, chronological splits, validation-based selection, and the weekly baseline. Read the actual final test metrics from the report.

**6–7 min: Verification.** Show Spark/MapReduce agreement, energy conservation and temporal leakage tests. Show why the incomplete last day prevents a credible next-day forecast.

**7–8 min: Limits and future work.** One home, historical observations, one worker, no labeled faults. Suggest weather features, more households, multi-node tests and prediction intervals.

## Likely questions

**Why Hadoop if this file fits on one laptop?** To demonstrate HDFS storage, resource-managed execution and two distributed processing frameworks. The project does not claim that Hadoop is necessary for a 133 MB file or that one worker proves speedup.

**What is the difference between HDFS and YARN?** HDFS manages files and data blocks. YARN manages application containers and resource allocation. Spark and MapReduce are processing frameworks scheduled by YARN.

**What does the NameNode do?** It manages filesystem metadata and block locations. The DataNode stores blocks. Neither trains the forecasting model.

**What does the ResourceManager do?** It schedules resources across applications. The NodeManager starts and supervises containers on a worker. Each application has an ApplicationMaster.

**Is this really distributed?** It executes through real YARN containers and HDFS, but all services are on a single machine. This is a pseudo-distributed deployment. The application evidence confirms use of the tools, not multi-machine scalability.

**Why Parquet and year/month partitioning?** Parquet stores typed columnar data. Queries can read only needed columns and prune month partitions. Spark distributes the work; SQL `EXPLAIN` can show partition filters.

**Why not sum power to obtain energy?** kW is a rate. Each reading represents one minute, so kWh contribution is kW times 1/60 hour. Sub-meterings are already minute Wh and need only conversion to kWh.

**How are missing values handled?** Retained as null. Aggregates sum observed energy and count valid readings. Low-coverage days and lag windows are excluded from modeling. No fabricated observations are introduced.

**Why avoid random train/test split?** It mixes future and past and makes temporal performance look better than deployment. Chronological boundaries simulate forecasting future dates.

**Does using test-period lagged observations leak?** In rolling one-day-ahead forecasting, yesterday's real observation is known today. It is legitimate. It would be inappropriate for a multi-day forecast made before those observations exist, which this project does not claim.

**What if the baseline wins?** It participates in validation-based selection. Simplicity is acceptable when it predicts better; we do not force a complex model to win.

**How can the two jobs agree if readings are missing?** Both sum only numeric active power, convert by 1/60, and count only valid measurements. The mapper preserves date keys even when a whole day is missing.

**Why do residual circuit values sometimes go negative?** Independently recorded and rounded values can disagree slightly. We keep and count them rather than silently clamp them and distort energy accounting.

**Are anomalies equipment failures?** No. A robust deviation flag identifies unusual consumption. Without labeled faults, it cannot establish cause or detection accuracy.

**What tests matter most?** Energy conservation, valid/missing count reconciliation, independent Spark/MapReduce agreement, and proving future edits cannot alter past prediction inputs.

**Where is the evidence?** `results/manifest.json`, `results/yarn_applications.json`, `results/verification.json`, `results/hdfs_status.txt`, and `runtime/logs/`.
