from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
# Create a SparkSession

spark = SparkSession.builder.appName("praktuchna3").getOrCreate()
df = spark.read.csv("data/trip_data_1.csv", header=True, inferSchema=True)

min_longitude = -74.05
max_longitude = -73.75

filtered_df = df.filter((df['pickup_longitude'] >= min_longitude) & (df['pickup_longitude'] <= max_longitude) &
                        (df['dropoff_longitude'] >= min_longitude) & (df['dropoff_longitude'] <= max_longitude))

filtered_df = filtered_df.filter((df['trip_distance'] >= 0) & (df['trip_distance'] <= 2))

filtered_df = filtered_df.withColumn("longitude_difference", expr("pickup_longitude - dropoff_longitude"))

grouped_df = df.groupBy("pickup_longitude")

joined_df = df.join(grouped_df, 'medallion', 'inner')

joined_df = joined_df.withColumn("longitude_difference", expr("pickup_longitude - dropoff_longitude"))

df = joined_df.filter((df['longitude_difference'] > 0.1))

df.show()
spark.stop()