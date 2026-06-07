#Part 3(Data Modelling)
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, lead, to_date, when, lit, create_map

# Initialize Spark Session
spark = SparkSession.builder.appName("part3_SubscriptionLifecycle").getOrCreate()

# 1. Create the Sample Input DataFrame
data = [
    (1, "10-Jan-22", "10-Mar-22", "Mobile"),
    (1, "05-Apr-22", "05-May-22", "Premium"),
    (1, "05-May-22", "05-Aug-22", "Standard"),
    (1, "01-Sep-22", None, "Premium")
]
columns = ["UserID", "FromDate", "ToDate", "Plan"]
df = spark.createDataFrame(data, columns)

# Convert string dates to actual DateTypes for accurate time-gap calculations
df = df.withColumn("FromDate", to_date(col("FromDate"), "d-MMM-yy")) \
       .withColumn("ToDate", to_date(col("ToDate"), "d-MMM-yy"))

# 2. Define a map to numerically rank the plan tiers from low to high
plan_rank = create_map([lit(x) for x in ["Mobile", 1, "Basic", 2, "Standard", 3, "Premium", 4]])
df = df.withColumn("PlanRank", plan_rank[col("Plan")])

# 3. Create a Window grouped by User and ordered chronologically
window_spec = Window.partitionBy("UserID").orderBy("FromDate")

# Get historical and future flags for transitions
df_enriched = df.withColumn("prev_ToDate", lag("ToDate").over(window_spec)) \
                .withColumn("prev_PlanRank", lag("PlanRank").over(window_spec)) \
                .withColumn("next_FromDate", lead("FromDate").over(window_spec))

# -------------------------------------------------------------------------
# Stream 1: Calculate Start Events (Subscribed / Upgraded / Downgraded)
# -------------------------------------------------------------------------
start_events = df_enriched.withColumn(
    "Action",
    when((col("prev_ToDate").isNull()) | (col("prev_ToDate") < col("FromDate")), "Subscribed")
    .when(col("PlanRank") > col("prev_PlanRank"), "Upgraded")
    .when(col("PlanRank") < col("prev_PlanRank"), "Downgraded")
).select(
    col("UserID"),
    col("FromDate").alias("ActionDate"),
    col("Action")
)

# -------------------------------------------------------------------------
# Stream 2: Calculate End Events (Cancelled)
# -------------------------------------------------------------------------
end_events = df_enriched.filter(
    col("ToDate").isNotNull() &
    ((col("next_FromDate").isNull()) | (col("next_FromDate") > col("ToDate")))
).select(
    col("UserID"),
    col("ToDate").alias("ActionDate"),
    lit("Cancelled").alias("Action")
)

# -------------------------------------------------------------------------
# Combine, Sort, and Display the final result
# -------------------------------------------------------------------------
final_output_df = start_events.union(end_events).orderBy("UserID", "ActionDate")

final_output_df.coalesce(1).write\
    .mode('overwrite')\
    .option('header', True)\
    .csv("C:/Users/Sakshi/PycharmProjects/PythonProject/OSN/out/out_3")
