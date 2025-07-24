from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, min, max, round, abs

# Create SparkSession
spark = SparkSession.builder \
    .appName("NumericFunctionsDemo") \
    .getOrCreate()

# Sample data (id, name, salary, bonus)
data = [
    (1, "Alice", 5000.75, -500),
    (2, "Bob", 4000.60, 300),
    (3, "Charlie", 6000.90, -200),
    (4, "David", 3500.30, 250)
]

columns = ["id", "name", "salary", "bonus"]

df = spark.createDataFrame(data, columns)
print("=== Original Data ===")
df.show()

# Apply numeric functions
from pyspark.sql.functions import col

df_transformed = df.select(
    "id", "name", "salary", "bonus",

    #  Sum of salary + bonus
    (col("salary") + col("bonus")).alias("total_income"),

    #  Round salary
    round("salary", 0).alias("rounded_salary"),

    #  Absolute bonus value
    abs("bonus").alias("abs_bonus")
)

print("=== Transformed Data ===")
df_transformed.show()

# Aggregation functions
df.select(
    sum("salary").alias("Total_Salary"),
    avg("salary").alias("Average_Salary"),
    min("salary").alias("Min_Salary"),
    max("salary").alias("Max_Salary")
).show()
