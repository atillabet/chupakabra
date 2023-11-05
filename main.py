from pyspark.sql import SparkSession
import transformation_stage

# Create a SparkSession
spark = SparkSession.builder \
    .appName("PySparkTest") \
    .getOrCreate()

# Create a test DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 29)]
schema = ["name", "age"]
df = spark.createDataFrame(data, schema)

# Display the DataFrame
df.show()

# Stop the SparkSession (important to release resources)
spark.stop()

if __name__ == '__main__':
    transformation_stage.run()