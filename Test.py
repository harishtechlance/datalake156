from pyspark.sql import SparkSession

# Create a Spark session
spark  = (SparkSession.builder
    .appName("TestSpark")
    .master("local[*]")
    .getOrCreate()
)

emp_data = [
    ("101", "Sharon", "F", 3000, "IT"),
    ("102", "David", "M", 4000, "IT"),
    ("103", "Mary", "F", 4000, "HR"),
    ("104", "John", "M", 5000, "HR"),
    ("105", "Jane", "F", 4500, "Sales"),
    ("106", "Mike", "M", 3500, "Sales")
]
emp_schema_string = "employee_id STRING, first_name STRING, gender STRING, salary INT, department STRING"

emp = spark.createDataFrame(data=emp_data, schema=emp_schema_string)

emp.rdd.getNumPartitions()  # Check number of partitions

emp.show()

emp_final = emp.where("salary >= 4000")
emp_final.write.format("csv").mode("overwrite").option("header", "true").save("output/emp_filtered")