#Part 1(Spark API) Task 3
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col, desc

spark = SparkSession.builder.appName('part1_task3').getOrCreate()

df = spark.read.csv(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\groceries.csv', header=False, inferSchema=True)

grocery_cols = ['_c0','_c1','_c2','_c3']

combined_col = df.select(explode(array([col(c) for c in grocery_cols])).alias("product"))

unique_grocery_items_notNull = combined_col.filter(col('product').isNotNull())

top_products_df = unique_grocery_items_notNull.groupBy('product').count().orderBy(desc('count')).limit(5)

top_5_records = top_products_df.collect()

output_filename  = "C:/Users/Sakshi/PycharmProjects/PythonProject/OSN/out/out_1_3.txt"

with open(output_filename, "w", encoding="utf-8") as file:

    for row in top_5_records:
        product_name = row["product"]
        frequency = row["count"]

        file.write(f"('{product_name}', {frequency})\n")
