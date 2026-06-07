#Part 1(Spark API) Task 1
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('part1_task1').getOrCreate()

df = spark.read.csv(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\groceries.csv', header=False, inferSchema=True)

df.show()
