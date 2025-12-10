import os
from pyspark.sql import SparkSession

# Configuration for network accessibility
SPARK_MASTER_HOST = "192.168.4.156"  # Linux VM IP
SPARK_DRIVER_HOST = "192.168.4.156"  # Linux VM IP (where PySpark runs)
WINDOWS_MACHINE_IP = "192.168.4.10"  # Your Windows machine IP

def create_spark_session(app_name="MyApp", master_url=None):
    """
    Create a Spark session with network-accessible configuration.
    
    Args:
        app_name: Name of the Spark application
        master_url: Optional Spark master URL. If None, uses local mode.
        
    Returns:
        SparkSession object
    """
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.host", SPARK_DRIVER_HOST) \
        .config("spark.driver.bindAddress", "0.0.0.0") \
        .config("spark.ui.port", "4040") \
        .config("spark.ui.enabled", "true")
    
    if master_url:
        builder = builder.master(master_url)
    else:
        # Local mode with all cores
        builder = builder.master("local[*]")
    
    spark = builder.getOrCreate()
    sc = spark.sparkContext
    
    print(f"Spark Application: {app_name}")
    print(f"Spark Version: {spark.version}")
    print(f"Master: {sc.master}")
    print(f"App ID: {sc.applicationId}")
    print(f"\n--- UI Access from Windows Machine ({WINDOWS_MACHINE_IP}) ---")
    print(f"Spark UI: http://{SPARK_DRIVER_HOST}:4040")
    print(f"(Access from Windows: http://{WINDOWS_MACHINE_IP}:4040)")
    print("=" * 70)
    
    return spark
