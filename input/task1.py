from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType

def task1_explore_data():
    spark = SparkSession.builder.appName("Task1_Explore").getOrCreate()

    # Define the schema explicitly
    schema = StructType([
        StructField("sensor_id", IntegerType(), True),
        StructField("timestamp", StringType(), True),  # Will convert later
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("sensor_type", StringType(), True)
    ])

    # Load CSV with schema
    df = spark.read.csv("sensor_data.csv", header=True, schema=schema)

    # Convert timestamp column to proper TimestampType
    df = df.withColumn("timestamp", df["timestamp"].cast(TimestampType()))

    # Create a temporary view
    df.createOrReplaceTempView("sensor_readings")

    print("First 5 rows:")
    first_five=spark.sql("SELECT * FROM sensor_readings LIMIT 5")
    first_five.show()

    total_records = spark.sql("SELECT COUNT(*) AS total_records FROM sensor_readings")
    total_records.show()

    distinct_locations = spark.sql("SELECT DISTINCT location FROM sensor_readings")
    distinct_locations.show()

    # Save DataFrame as CSV
    output_folder = "output/task1/"

    distinct_locations.write.mode("overwrite").csv(f"{output_folder}task1a_output.csv", header=True)
    total_records.write.mode("overwrite").csv(f"{output_folder}task1b_output.csv", header=True)
    first_five.write.mode("overwrite").csv(f"{output_folder}task1c_output.csv", header=True)


    spark.stop()

if __name__ == "__main__":
    task1_explore_data()

