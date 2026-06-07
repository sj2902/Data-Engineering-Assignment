# Data-Engineering-Assignment
## Development & Submission Details

This project was developed and tested locally using **PyCharm**. The complete source code, configuration files, and execution outputs have been uploaded to this GitHub repository.

### Execution & Outputs
To demonstrate that the pipeline runs successfully without requiring a full local setup from the reviewer, the generated outputs have been included in this repository:
* **Source Code:** Located in the `src/` directory.
* **Execution Outputs:** Expected output data can be found in the `out/` directory.

---
## Part 1: Spark API — Grocery Transactions Analysis

This section leverages Spark's DataFrame API to process transactional shopping data, handle varying row lengths, and generate key business summaries.

### Local Environment Setup Note
The code was implemented and tested locally in **PyCharm**. While absolute file paths (e.g., `C:\Users\Sakshi\...`) are used in the scripts to read the raw data and write text outputs directly to the local file system, the core logic is fully modular and engine-agnostic.

---

### Task 1: Data Ingestion & Schema Inference
* **Objective:** Load the raw unstructured/semi-structured grocery CSV file into Spark and inspect its initial format.
* **Logic Brief:** A Spark session is initialized, and the raw dataset is ingested using `spark.read.csv()` with `inferSchema=True`. Because transactional records contain varying numbers of items per line, Spark dynamically loads them into indexed columns (e.g., `_c0`, `_c1`, etc.), which can then be displayed using `df.show()`.

---

### Task 2a: Unique Product List Generation
* **Objective:** Extract every unique product across all transaction columns and write the distinct list to `out/out_1_2a.txt`.
* **Logic Brief:** 1. To handle the tabular representation of transactional data, an array mapping the relevant item columns (`_c0` to `_c3`) is created.
  2. The `array()` and `explode()` functions are used to flatten the wide rows into a single column (`grocery_items`).
  3. `filter(col(...).isNotNull())` drops empty items, and `.distinct()` isolates the unique product catalog.
  4. The unique items are collected and saved cleanly as individual lines using standard Python file I/O operations.

---

### Task 2b: Total Unique Product Count
* **Objective:** Calculate the total count of distinct products and write it to `out/out_1_2b.txt` in the format `Count: X`.
* **Logic Brief:** Following the exact flattening and deduplication pipeline established in Task 2a, the Spark DataFrame transformation action `.count()` is executed on the unique products dataset. This safely aggregates the total count distributedly, which is then written out to the required target text file matching the specified format.

---

### Task 3: Top 5 Most Frequently Purchased Products
* **Objective:** Determine the top 5 most popular products based on transaction frequency and export the results in descending order to `out/out_1_3.txt` as a tuple-like string.
* **Logic Brief:** 1. The transaction table is flattened using the `explode` strategy to convert individual product entries into rows.
  2. A PySpark `.groupBy('product').count()` aggregation is executed to calculate the exact frequency of every item in the dataset.
  3. The aggregated data is sorted in descending order using `orderBy(desc('count'))` and restricted to the top 5 records using `.limit(5)`.
  4. Finally, `.collect()` retrieves the 5 rows locally, and a Python loop formats them into the exact string representation (`('product_name', frequency)`) required by the assignment guidelines.



## Part 2: Spark DataFrame API — Airbnb Dataset Analysis

This section utilizes Spark’s DataFrame API to perform data ingestion, statistical aggregations, conditional filtering, and custom file writing on structured Airbnb metrics.

### Local Environment Setup Note
All tasks were developed locally using **PyCharm**. The scripts read from local snappy-compressed Parquet storage and write out results directly to a local target folder structure via absolute paths.

---

### Tasks 1 & 2: Dataset Ingestion & High-Level Summary Metrics
* **Objective:** Ingest the raw Parquet file and output a single CSV file (`out/out_2_2`) detailing the minimum price, maximum price, and overall row count.
* **Logic Brief:** 1. The Airbnb snappy-parquet file is loaded into Spark using `spark.read.format('parquet')`.
  2. The `.agg()` function executes global aggregation queries using `min("price")`, `max("price")`, and `count("*")` with appropriate aliases.
  3. The resulting dataframe is consolidated into a single partition using `.coalesce(1)` and exported as a CSV with headers enabled using `.write.mode("overwrite").csv()`.

---

### Task 3: Filtered Statistical Averages (Luxury Properties)
* **Objective:** Calculate the average number of bathrooms and bedrooms for high-value properties (Price > 5000 and Review Value == 10) and export to `out/out_2_3`.
* **Logic Brief:** 1. The codebase applies `.filter()` using a bitwise logical condition (`&`) on the `price` and `review_scores_value` columns.
  2. The `avg()` aggregation calculates the metrics on the filtered dataset, generating `avg_bathrooms` and `avg_bedrooms`.
  3. `.coalesce(1)` forces a single-partition output to write out a clean, unified CSV folder structure.

---

### Task 4: Target Selection (Best Value Property Capacity)
* **Objective:** Find the capacity (`accommodates`) of the lowest-priced, highest-rated property and output the single numerical value to `out/out_2_4.txt`.
* **Logic Brief:** 1. `.orderBy()` sequences the data by `price` in ascending order (cheapest first) and `review_scores_rating` in descending order (highest rating first).
  2. The script extracts the top record using `.limit(1)` and casts the target column `accommodates` to a string data type.
  3. It leverages Spark's `.write.text()` API combined with `.coalesce(1)` and `header=False` to save the raw value as an un-headed text representation into the specified directory.



## Part 3: Data Modeling — Subscription Lifecycle Tracking

This section addresses the data modeling challenge of capturing complex state changes (Subscribed, Upgraded, Downgraded, and Churned/Cancelled) across a streaming service user lifecycle. 

### Why the Kimball Dimensional Modeling Approach Was Used
To solve this problem, a **Kimball Type 2 Slowly Changing Dimension (SCD Type 2)** approach was applied conceptually:
* **Historical Tracking:** Instead of overwriting user states, the model relies on a date-effective tracking schema (`FromDate` to `ToDate`). This accurately preserves a user's chronological footprint over time.
* **Granular Lifecycle States:** Rather than managing states as a rigid table, the pipeline separates continuous active periods into an **Event/Accumulating Snapshot** model. This permits instant business analysis on conversion velocities (e.g., measuring the exact duration between a subscription and an upgrade).
* **Business-Driven Hierarchy:** By establishing numerical rankings for categorical plans (Mobile to Premium), the system treats plans as a conforming, ranked attribute dimension.

---

### Code Execution & Logic Brief
* **Objective:** Ingest transactional subscription logs, calculate historical transitions, and generate a continuous stream of structured behavioral lifecycle events.
* **Logic Breakdown:**
  1. **Schema Standardization:** The script ingests the user table and casts textual date strings into explicit `DateType` schemas to avoid calculation anomalies.
  2. **Tier Mapping:** A dynamic map structure (`create_map`) assigns numerical tier weights (`Mobile = 1` up to `Premium = 4`) to easily mathematically compute directional switches.
  3. **Window Analytics:** An analytical `Window` grouped by `UserID` and ordered chronologically allows the script to evaluate adjacent records globally via `lag()` and `lead()` functions.
  4. **Parallel Stream Split & Union:** * **Stream 1 (Start Events):** Evaluates timeline boundaries and tier changes to isolate **Subscribed**, **Upgraded**, or **Downgraded** event timestamps.
     * **Stream 2 (End Events):** Filters out future continuations to accurately stamp a boundary **Cancelled** event whenever a time gap or hard termination is encountered.
  5. **Consolidation:** The streams are unified using `.union()`, ordered sequentially by user timeline, consolidated via `.coalesce(1)`, and written to `out/out_3` as a structured analytical log.
