#Part 2 (Spark Dataframe API) Task 4
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('part2_task4').getOrCreate()

airbnb_df = spark.read.format('parquet').option('header',True).option('inferSchema',True).load(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\part2\part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet')

ordered_df = airbnb_df.orderBy(col('price').asc(), col('review_scores_rating').desc())

result_df = ordered_df.select(col('accommodates').cast('string')).limit(1)

result_df.coalesce(1).write\
    .mode('overwrite')\
    .option('header', False)\
    .text("C:/Users/Sakshi/PycharmProjects/PythonProject/OSN/out/out_2_4.txt")
