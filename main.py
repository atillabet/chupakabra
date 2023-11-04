from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
# Create a SparkSession

spark = SparkSession.builder.appName("praktuchna3").getOrCreate()
df = spark.read.csv("data/trip_data_1.csv", header=True, inferSchema=True)

min_latitude = 40.6
max_latitude = 40.9

filtered_df = df.filter((df['pickup_latitude'] >= min_latitude) & (df['pickup_latitude'] <= max_latitude) &
                        (df['dropoff_latitude'] >= min_latitude) & (df['dropoff_latitude'] <= max_latitude))

filtered_df = filtered_df.filter((df['trip_distance'] >= 0) & (df['trip_distance'] <= 2))

filtered_df = filtered_df.withColumn("latitude_difference", expr("pickup_latitude - dropoff_latitude"))

grouped_df = filtered_df.groupBy("pickup_latitude")

joined_df = filtered_df.join(grouped_df, "pickup_latitude", "inner")

joined_df = joined_df.withColumn("latitude_difference", expr("pickup_latitude - dropoff_latitude"))

filtered_df = joined_df.filter((joined_df['latitude_difference'] > 0.1))

filtered_df.show()
spark.stop()