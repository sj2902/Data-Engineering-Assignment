#Part 1(Spark API) Task 2b
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col

spark = SparkSession.builder.appName('part1_task2b').getOrCreate()

df = spark.read.csv(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\groceries.csv', header=False, inferSchema=True)

grocery_cols = ['_c0','_c1','_c2','_c3']

combined_col = df.select(explode(array([col(c) for c in grocery_cols])).alias("grocery_items"))

unique_grocery_items = combined_col.filter(col('grocery_items').isNotNull()).distinct()

unique_grocery_list = [row["grocery_items"] for row in unique_grocery_items.collect()]

product_count = unique_grocery_items.count()

output_dir = r'C:/Users/Sakshi/PycharmProjects/PythonProject/OSN/out/out_1_2b.txt'

with open(output_dir, "w", encoding="utf-8") as f:
    f.write(f"Count: {product_count}\n")
