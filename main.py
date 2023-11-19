from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, sum, count, expr, min, max, expr

spark = SparkSession.builder.appName("praktuchna3").getOrCreate()
df = spark.read.csv("data/trip_data_1.csv", header=True, inferSchema=True)

# Question: What is the average rate score for trips with more than 2 passengers?
filtered_df_2 = df.filter((df['passenger_count'] > 2))
result_df_q1 = filtered_df_2.groupBy('medallion').agg(expr('avg(rate_score)').alias('average_rate_score'), expr('count(rate_score)').alias('total_trips_with_more_than_two_passengers'))

# Question: What is the total number of trips and the average rate score for each passenger count?
result_df_q2 = df.groupBy('passenger_count').agg(expr('count(medallion)').alias('total_trips'), expr('avg(rate_score)').alias('average_rate_score'))

# Question: For each medallion, what is the difference between the maximum and minimum rate scores?
result_df_q3 = df.groupBy('medallion').agg((expr('max(rate_score)') - expr('min(rate_score)')).alias('rate_score_difference'))

# Question: How many passengers had a trip with a rate that doesn't equal 1?
result_df_additional = df.filter(df['rate_score'] != 1).agg(expr('sum(passenger_count)').alias('total_passengers'))

result_df_additional.show()

df.show()
spark.stop()