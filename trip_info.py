from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, max, min, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import DecimalType

spark = SparkSession.builder.appName("TaxiDataQueries").getOrCreate()
dataset = spark.read.csv('path/to/your/NYC_Taxi_Trip_Data_2013.csv', header=True)

dataset = dataset.withColumn("trip_time_in_secs", col("trip_time_in_secs").cast("int"))
dataset = dataset.withColumn("trip_distance", col("trip_distance").cast(DecimalType(10, 2)))

# Question 1: Filter based on conditions
query1_result = dataset.filter((col("hack_license").isNotNull()) & (col("trip_time_in_secs") > 0) & (col("trip_distance") > 0.0))

# Question 2: Join with another dataset (assuming another dataset is available)
# For illustration, let's assume there's another dataset named 'additional_data'
# with columns 'hack_license' and 'additional_info'
additional_data = spark.read.csv('path/to/your/additional_data.csv', header=True)
query2_result = dataset.join(additional_data, on="hack_license", how="inner")

# Question 3: Group by hack_license and store_and_fwd_flag, calculate the total trip time and distance
query3_result = dataset.groupBy("hack_license", "store_and_fwd_flag")\
                      .agg(sum("trip_time_in_secs").alias("total_trip_time"),
                           sum("trip_distance").alias("total_trip_distance"))

# Question 4: Window function to find the maximum trip time for each hack_license
window_spec = Window.partitionBy("hack_license").orderBy(col("trip_time_in_secs").desc())
query4_result = dataset.withColumn("max_trip_time",
                                   max("trip_time_in_secs").over(window_spec))\
                      .filter(col("trip_time_in_secs") == col("max_trip_time"))\
                      .select("hack_license", "trip_time_in_secs")

# Question 5: Window function to calculate the rank of each record based on trip_distance
window_spec = Window.orderBy(col("trip_distance").desc())
query5_result = dataset.withColumn("rank", row_number().over(window_spec))\
                      .filter(col("rank") == 1)\
                      .select("hack_license", "trip_distance")

# Question 6: Group by store_and_fwd_flag, calculate the average trip time and distance
query6_result = dataset.groupBy("store_and_fwd_flag")\
                      .agg(avg("trip_time_in_secs").alias("avg_trip_time"),
                           avg("trip_distance").alias("avg_trip_distance"))

query1_result.show()
query2_result.show()
query3_result.show()
query4_result.show()
query5_result.show()
query6_result.show()

spark.stop()

if __name__ == '__main__':
    run()