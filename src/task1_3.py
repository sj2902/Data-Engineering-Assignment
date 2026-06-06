from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, array, col, desc

spark = SparkSession.builder.appName('Top5').getOrCreate()

df = spark.read.csv(r'C:\Users\Sakshi\PycharmProjects\PythonProject\OSN\raw_data\groceries.csv', header=False, inferSchema=True)

grocery_cols = ['_c0','_c1','_c2','_c3']

combined_col = df.select(explode(array([col(c) for c in grocery_cols])).alias("product"))

unique_grocery_items_notNull = combined_col.filter(col('product').isNotNull())

top_products_df = unique_grocery_items_notNull.groupBy('product').count().orderBy(desc('count')).limit(5)


# 4. Format rows to match the required tuple layout: ('product_name', count)
# To create an open bracket line, item line, and close bracket line, we collect them cleanly.
top_5_records = top_products_df.collect()

# 5. Write out the results as a normal text file using the specific layout
output_filename  = "C:/Users/Sakshi/PycharmProjects/PythonProject/OSN/out/out_1_3.txt"

with open(output_filename, "w", encoding="utf-8") as file:

    for row in top_5_records:
        product_name = row["product"]
        frequency = row["count"]

        # Match your exact structural spacing and bracket layout
        file.write(f"('{product_name}', {frequency})\n")

print(f"Successfully generated top products file at: {output_filename}")