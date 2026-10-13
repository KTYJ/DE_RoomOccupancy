from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, window, max as spark_max, hour, date_format, when
from utils.space_optimization import compute_space_optimization
from utils.environment_control import compute_environment_summary
from utils.comfort_index import compute_environment_comfort_index
from utils.energy_usage import compute_avg_monthly_energy


class RBBatchProcessing:


    def __init__(self, input_path, output_path, app_name="Library_Historical_Batch_Analysis"):
        self.input_path = input_path
        self.output_path = output_path

        self.spark = SparkSession.builder.appName(app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        print(f"Spark Session initialized for app: {app_name}")

    def run(self):

        print("Reading curated data from HDFS...")
        df = self.spark.read.parquet(self.input_path)
        df.cache()

        print("Checking useless sensor columns...")
        numeric_cols = [
            c for c, t in df.dtypes
            if t in ["int", "double", "float", "bigint"] and c not in ["room"]
        ]

        agg_exprs = [spark_max(col(c)).alias(c) for c in numeric_cols]
        stats_row = df.agg(*agg_exprs).collect()[0]
        zero_cols = [c for c in numeric_cols if stats_row[c] == 0]

        if zero_cols:
            print("Dropping columns:", zero_cols)
            cleaned_overall_sensor = df.drop(*zero_cols)
        else:
            print("No useless columns found.")
            cleaned_overall_sensor = df

        print("\n--- Cleaned Overall Sensor Table (Preview 20 rows) ---")
        cleaned_overall_sensor.show(20, truncate=False)

        print("Saving cleaned overall sensor table...")
        cleaned_overall_sensor.write.mode("overwrite").parquet(self.output_path + "cleaned_overall_sensor")

        print("\nComputing 1-hour moving average...")
        current_numeric_cols = [
            c for c, t in cleaned_overall_sensor.dtypes
            if t in ["int", "double", "float", "bigint"] and c not in ["room"]
        ]

        ma_df = cleaned_overall_sensor.groupBy(
            window(col("ts"), "1 hour"),
            col("room")
        ).agg(
            *[round(avg(col(c)), 2).alias(c) for c in current_numeric_cols]
        ).orderBy("window")

        ma_df = ma_df.withColumn("start_time", col("window.start")) \
                     .withColumn("end_time", col("window.end")) \
                     .drop("window")

        cols_order = ["start_time", "end_time", "room"] + current_numeric_cols
        ma_df = ma_df.select(*cols_order)

        print("\n--- 1-Hour Moving Average Table (Preview 20 rows) ---")
        ma_df.show(20, truncate=False)

        print("Saving moving average table...")
        ma_df.write.mode("overwrite").parquet(self.output_path + "moving_average_1hr")

        rooms = cleaned_overall_sensor.select("room").distinct().rdd.flatMap(lambda x: x).collect()

        for room in rooms:
            room_df = ma_df.filter(col("room") == room)
            print(f"Saving Room {room} sensor table...")
            room_df.write.mode("overwrite").parquet(self.output_path + f"{room}_sensor")

        print("\nComputing Aggregation 1: Hourly Trends...")
        agg_hourly = cleaned_overall_sensor.groupBy(hour("ts").alias("hour_of_day")) \
            .agg(round(avg("occ_cnt"), 2).alias("avg_occupancy")) \
            .withColumn(
                "category",
                when(col("avg_occupancy") < 2, "Low")
                .when(col("avg_occupancy") <= 5, "Medium")
                .otherwise("High")
            ).orderBy("hour_of_day")

        agg_hourly.show(24)
        agg_hourly.write.mode("overwrite").parquet(self.output_path + "agg_hourly_trends")


        print("\nComputing Aggregation 2: Day of Week Trends...")
        agg_daily = cleaned_overall_sensor.groupBy(date_format("ts", "EEEE").alias("day_of_week")) \
            .agg(round(avg("occ_cnt"), 2).alias("avg_occupancy")) \
            .withColumn(
                "category",
                when(col("avg_occupancy") < 2, "Low")
                .when(col("avg_occupancy") <= 5, "Medium")
                .otherwise("High")
            ).orderBy(
                when(col("day_of_week") == "Monday", 1)
                .when(col("day_of_week") == "Tuesday", 2)
                .when(col("day_of_week") == "Wednesday", 3)
                .when(col("day_of_week") == "Thursday", 4)
                .when(col("day_of_week") == "Friday", 5)
                .when(col("day_of_week") == "Saturday", 6)
                .otherwise(7)
            )

        agg_daily.show()
        agg_daily.write.mode("overwrite").parquet(self.output_path + "agg_daily_trends")

        space_df = compute_space_optimization(cleaned_overall_sensor)
        space_df.write.mode("overwrite").parquet(self.output_path + "space_optimization")

        print("\nComputing General Environmental Summary...")
        summary_df = compute_environment_summary(cleaned_overall_sensor, self.spark)
        summary_df.show(truncate=False)
        summary_df.write.mode("overwrite").parquet(self.output_path + "environment_control")

        print("\nComputing Environmental Comfort Index...")
        comfort_df = compute_environment_comfort_index(cleaned_overall_sensor)
        comfort_df.show(truncate=False)
        comfort_df.write.mode("overwrite").parquet(self.output_path + "environment_comfort_index")

        print("\nComputing Average Monthly Energy Consumption Across Rooms...")
        avg_monthly_energy_df = compute_avg_monthly_energy(cleaned_overall_sensor)
        avg_monthly_energy_df.show(12, truncate=False)
        avg_monthly_energy_df.write.mode("overwrite").parquet(self.output_path + "avg_monthly_energy")

        self.spark.stop()
        print("\nBatch processing complete!")


if __name__ == "__main__":
    input_path = "/user/hduser/occupancy/robod_curated_v3/"
    output_path = "/user/hduser/occupancy/batch_output/"

    batch_processor = RBBatchProcessing(input_path, output_path)
    batch_processor.run()