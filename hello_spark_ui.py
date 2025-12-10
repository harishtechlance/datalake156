from spark_config import create_spark_session

# Create a Spark session with network-accessible UI
spark = create_spark_session("HelloSparkWithUI")

# Create a simple DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Display the DataFrame
print("\nDataFrame:")
df.show()

# Filtering
print("\nPeople older than 25:")
df.filter(df.Age > 25).show()

# Aggregation
total_age = df.agg({'Age': 'sum'}).collect()[0][0]
print(f"\nTotal age: {total_age}")

print("\n" + "=" * 70)
print("Spark UI is running and accessible from your Windows machine!")
print("While this script is running, you can open the Spark UI in your browser.")
print("=" * 70)

# Keep the session alive for a few seconds so you can access the UI
import time
print("\nScript will complete in 5 seconds...")
time.sleep(5)

# Stop the Spark session
spark.stop()
print("Spark session stopped.")
