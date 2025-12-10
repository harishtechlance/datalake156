from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .getOrCreate()

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Display the DataFrame
print("Hello from PySpark!")
print("=" * 50)
df.show()

# Basic operations
print("\nFiltering people older than 25:")
df.filter(df.Age > 25).show()

print("\nSum of ages:")
print(f"Total age: {df.agg({'Age': 'sum'}).collect()[0][0]}")

# Stop the Spark session
spark.stop()
