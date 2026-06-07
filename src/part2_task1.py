#Part 2(Spark Dataframe API) Task 1
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('part2_task1').getOrCreate()

airbnb_df = spark.read.format('parquet').option('header',True).option('inferSchema',True).load(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\part2\part-00000-tid-4320459746949313749-5c3d407c-c844-4016-97ad-2edec446aa62-6688-1-c000.snappy.parquet')

airbnb_df.show()
