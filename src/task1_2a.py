from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col

spark = SparkSession.builder.appName('osn').getOrCreate()

df = spark.read.csv(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\groceries.csv', header=False, inferSchema=True)

grocery_cols = ['_c0','_c1','_c2','_c3']

combined_col = df.select(explode(array([col(c) for c in grocery_cols])).alias("grocery_items"))

unique_grocery_items = combined_col.filter(col('grocery_items').isNotNull()).distinct()

unique_grocery_list = [row["grocery_items"] for row in unique_grocery_items.collect()]

output_filename = r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\out\out_1_2a.txt'

lines = [row[0] for row in unique_grocery_items.collect()]

with open(output_filename, "w", encoding="utf-8") as file:
    for line in lines:
        file.write(f"{line}\n")