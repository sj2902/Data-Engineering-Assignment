#Part 2(Spark Dataframe API) Task 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('part2_task3').getOrCreate()

airbnb_df = spark.read.format('parquet').option('header',True).option('inferSchema',True).load(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\part2\part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet')

filtered_df = airbnb_df.filter((col('price') > 5000) & (col('review_scores_value') == 10))

avg_df = filtered_df.agg(
    avg(col('bathrooms')).alias('avg_bathrooms'),
    avg(col('bedrooms')).alias('avg_bedrooms')
)

avg_df.coalesce(1).write\
    .mode('overwrite')\
    .option('header', True)\
    .csv("C:/Users/Sakshi/PycharmProjects/PythonProject/OSN/out/out_2_3")
