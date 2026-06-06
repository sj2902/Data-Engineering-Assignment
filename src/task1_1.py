from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col

spark = SparkSession.builder.appName('task1_1').getOrCreate()

df = spark.read.csv(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\groceries.csv', header=False, inferSchema=True)

df.show()
