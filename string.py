from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, trim, ltrim, rtrim, substring_index

# Create Spark session
spark = SparkSession.builder \
    .appName("StringFunctionsDemo") \
    .getOrCreate()

# Sample data
data = [
    (1, "  alice  ", "alice@gmail.com"),
    (2, "  Bob", "bob@yahoo.com"),
    (3, "Charlie  ", "charlie@outlook.com"),
    (4, "  David ", "david@hotmail.com")
]

columns = ["id", "name", "email"]


df = spark.createDataFrame(data, schema=columns)

print("=== Original Data ===")
df.show(truncate=False)


df_transformed = df.select(
    "id",
    "name",
    "email",

   # Convert to UPPER case
    upper("name").alias("name_upper"),

    #  Trim both sides
    trim("name").alias("name_trim"),

    #  Left trim
    ltrim("name").alias("name_ltrim"),

    #  Right trim
    rtrim("name").alias("name_rtrim"),

    #  Substring before @ in email
    substring_index("email", "@", 1).alias("email_username")
)

print("=== Transformed Data ===")
df_transformed.show(truncate=False)
