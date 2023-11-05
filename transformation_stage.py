from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, unix_timestamp, abs, avg, count, max, to_date

### Transformation stage ###

# Emiliia Bondarenko #

# Helpers #


def last_day_in_dataset(dataset: DataFrame):
    df = dataset.alias("last_day_in_dataset")
    df = df.withColumn("date_pickup", to_date("pickup_datetime"))
    df = df.withColumn("date_dropoff", to_date("dropoff_datetime"))
    return df.filter(df["date_pickup"] == df["date_dropoff"]).agg(max("date_pickup").alias("the_last_day"))


# Queries #


def find_average_speed_for_each_passenger_count(dataframe: DataFrame):
    df = dataframe.alias("find_average_speed_for_each_passenger_number")
    df = df.groupBy("passenger_count")\
           .agg((avg(df["trip_distance"] / df["trip_time_in_secs"]) * 3600).alias("avg_speed_in_kmph"))\
           .orderBy("passenger_count")
    return df.select("passenger_count", "avg_speed_in_kmph")


def check_difference_in_trip_duration(dataframe: DataFrame):
    # Filter data that have been determined as invalid
    df = dataframe.alias("check_difference_trip_duration").filter(~dataframe['passenger_count'].isin(0, 9, 208))
    df = df.withColumn("trip_duration", (col("dropoff_datetime").cast("long") - col("pickup_datetime").cast("long")))
    df = df.withColumn("difference", abs(col("trip_duration") - col("trip_time_in_secs")))
    return df.select("passenger_count", "trip_time_in_secs", "trip_duration", "difference")


def count_number_of_records_for_each_unique_medallion_and_hack_license(dataframe: DataFrame):
    # Filter data that have been determined as invalid
    df = dataframe.alias("count_number_of_records_for_each_unique_medallion_and_hack_license")
    df = df.groupBy("medallion", "hack_license")\
           .agg((count(col("*")).alias("number_of_records")))\
           .orderBy("hack_license", "number_of_records", "medallion")
    return df.select("hack_license", "medallion", "number_of_records")


def find_trip_with_the_highest_duration_for_the_last_day_in_dataset(dataframe: DataFrame):
    df = dataframe.alias("find_trip_with_the_highest_duration_for_the_last_day_in_dataset")
    df = df.filter(~df['passenger_count'].isin(0, 9, 208))
    df = df.withColumn("date_pickup", to_date("pickup_datetime"))
    df = df.withColumn("date_dropoff", to_date("dropoff_datetime"))
    last_day = last_day_in_dataset(dataframe)
    df = df.filter(df["date_pickup"] == last_day.collect()[0][0])
    the_highest_duration = df.select(max("trip_time_in_secs")).collect()[0][0]
    df = df.filter(df["trip_time_in_secs"] == the_highest_duration)
    return df.select("medallion", "pickup_datetime", "dropoff_datetime", "trip_time_in_secs")


def compare_average_speed_with_average_speed_per_medallion(dataframe: DataFrame):
    df = dataframe.alias("compare_average_speed_with_average_speed_per_medallion")
    average_speed_for_each_passenger_count = find_average_speed_for_each_passenger_count(dataframe)
    df = df.groupBy("medallion", "passenger_count")\
        .agg((avg(df["trip_distance"] / df["trip_time_in_secs"]) * 3600).alias("avg_speed_in_kmph_per_medallion"))
    df = df.join(average_speed_for_each_passenger_count, df["passenger_count"] == average_speed_for_each_passenger_count["passenger_count"])
    return df.select("medallion", "compare_average_speed_with_average_speed_per_medallion.passenger_count",
                     "avg_speed_in_kmph_per_medallion", "avg_speed_in_kmph")


def find_the_biggest_differences_in_trip_duration_data(dataframe: DataFrame):
    difference_in_trip_duration = check_difference_in_trip_duration(dataframe)
    df = difference_in_trip_duration.orderBy(difference_in_trip_duration["difference"].desc()).limit(10)
    return df.select("*")


def run():
    # Common preprocessing #

    spark = SparkSession.builder \
        .appName("Transformation stage") \
        .getOrCreate()

    dataset = spark.read.csv('data/trip_data_1.csv', header=True)
    dataset = dataset.withColumn("pickup_datetime",
                                 unix_timestamp("pickup_datetime", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    dataset = dataset.withColumn("dropoff_datetime",
                                 unix_timestamp("dropoff_datetime", "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    dataset = dataset.withColumn("passenger_count", col("passenger_count").cast("int"))

    # Emiliia Bondarenko #

    # It looks like for passenger_count = 0, 9, 208 data is invalid (result of find_average_speed_for_each_passenger_number).
    # We can filter out these records in future.
    find_average_speed_for_each_passenger_count(dataset)\
        .write.mode('ignore').csv('data/find_average_speed_for_each_passenger_count.csv', header=True)

    check_difference_in_trip_duration(dataset)\
        .write.mode('ignore').csv('data/check_difference_in_trip_duration.csv', header=True)

    count_number_of_records_for_each_unique_medallion_and_hack_license(dataset)\
        .write.mode('ignore').csv('data/count_number_of_records_for_each_unique_medallion_and_hack_license.csv', header=True)

    find_trip_with_the_highest_duration_for_the_last_day_in_dataset(dataset)\
        .write.mode('ignore').csv('data/find_trip_with_the_highest_duration_for_the_last_day_in_dataset.csv', header=True)

    compare_average_speed_with_average_speed_per_medallion(dataset)\
        .write.mode('ignore').csv('data/compare_average_speed_with_average_speed_per_medallion.csv', header=True)

    # We can see some typos in data. It's not recommended to use "trip_duration" column.
    find_the_biggest_differences_in_trip_duration_data(dataset)\
        .write.mode('ignore').csv('data/find_the_biggest_differences_in_trip_duration_data.csv', header=True)

    spark.stop()


if __name__ == '__main__':
    run()
