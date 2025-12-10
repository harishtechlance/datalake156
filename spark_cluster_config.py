"""
Spark Cluster Configuration
For submitting jobs to a Spark cluster with Master/Worker setup
"""

SPARK_MASTER_URL = "spark://192.168.4.156:7077"  # Spark Master address
SPARK_DRIVER_HOST = "192.168.4.156"
WINDOWS_ACCESS_IP = "192.168.4.10"

# To use this in your scripts:
# from spark_cluster_config import SPARK_MASTER_URL
# spark = SparkSession.builder.master(SPARK_MASTER_URL).appName("MyApp").getOrCreate()

print(f"Spark Master: {SPARK_MASTER_URL}")
print(f"Access Master UI from Windows: http://{SPARK_MASTER_URL.split(':')[1][2:]}{':8080'}")