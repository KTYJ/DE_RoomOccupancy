from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, lit, coalesce, round, last, avg
from pyspark.sql.types import StructType, StructField, StringType
from processing.RBanalytics import RBanalytics
from pyspark.sql.window import Window


class LibraryOccupancyProcessor:
 
    def __init__(self, app_name="Library_Indoor_Processor"):

        self.spark = SparkSession.builder \
            .appName(app_name) \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("ERROR")

        self.hdfs_path = "/user/hduser/occupancy/robod_curated_v3/"
        self.checkpoint_path = "/user/hduser/occupancy/robod_checkpoints_v3/"

        self.schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("Room_ID", StringType(), True),

            StructField("voc", StringType(), True),
            StructField("sound_pressure_level", StringType(), True),
            StructField("indoor_relative_humidity", StringType(), True),
            StructField("air_temperature", StringType(), True),
            StructField("illuminance", StringType(), True),
            StructField("pm2.5", StringType(), True),
            StructField("indoor_co2", StringType(), True),

            StructField("wifi_connected_devices", StringType(), True),

            StructField("ceiling_fan_energy", StringType(), True),
            StructField("lighting_energy", StringType(), True),
            StructField("plug_load_energy", StringType(), True),
            StructField("chilled_water_energy", StringType(), True),
            StructField("ahu_fcu_fan_energy", StringType(), True),

            StructField("supply_air_flow", StringType(), True),
            StructField("damper_position", StringType(), True),
            StructField("temperature_setpoint", StringType(), True),
            StructField("cooling_coil_valve_position", StringType(), True),
            StructField("cooling_coil_valve_command", StringType(), True),
            StructField("ahu_fcu_fan_speed", StringType(), True),
            StructField("offcoil_air_temperature", StringType(), True),
            StructField("offcoil_temperature_setpoint", StringType(), True),
            StructField("supply_air_humidity", StringType(), True),
            StructField("pressure_across_filter", StringType(), True),
            StructField("supply_air_static_pressure", StringType(), True),
            StructField("supply_air_temperature", StringType(), True),

            StructField("occupant_count", StringType(), True)
        ])

    def read_from_kafka(self, bootstrap_servers, topic):

        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load()

    def transform_data(self, raw_df):

        parsed_df = raw_df.select(
            from_json(col("value").cast("string"), self.schema).alias("data")
        ).select("data.*")

        room_time_window = Window.partitionBy("room").orderBy("ts").rowsBetween(Window.unboundedPreceding, 0)
        room_window = Window.partitionBy("room")

        return parsed_df.select(

            to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm XXX").alias("ts"),
            col("Room_ID").alias("room"),

            
            round(coalesce(col("voc").cast("float"), lit(0.0)), 2).alias("voc_ppb"),
            round(coalesce(col("indoor_co2").cast("float"), lit(0.0)), 2).alias("co2_ppm"),
            round(coalesce(col("air_temperature").cast("float"), lit(0.0)), 2).alias("temp_c"),
            round(coalesce(col("indoor_relative_humidity").cast("float"), lit(0.0)), 2).alias("hum_pct"),
            round(coalesce(col("`pm2.5`").cast("float"), lit(0.0)), 2).alias("pm25_ugm3"),
            round(coalesce(col("sound_pressure_level").cast("float"), lit(0.0)), 2).alias("noise_dba"),
            round(coalesce(col("illuminance").cast("float"), lit(0.0)), 2).alias("lux"),

            coalesce(col("wifi_connected_devices").cast("int"), lit(0)).alias("wifi_dev_cnt"),
            coalesce(col("occupant_count").cast("int"), lit(0)).alias("occ_cnt"),

            round(coalesce(col("ceiling_fan_energy").cast("float"), lit(0.0)), 2).alias("fan_e_kwh"),
            round(coalesce(col("lighting_energy").cast("float"), lit(0.0)), 2).alias("light_e_kwh"),
            round(coalesce(col("plug_load_energy").cast("float"), lit(0.0)), 2).alias("plug_e_kwh"),
            round(coalesce(col("chilled_water_energy").cast("float"), lit(0.0)), 2).alias("chw_e_kwh"),
            round(coalesce(col("ahu_fcu_fan_energy").cast("float"), lit(0.0)), 2).alias("hvac_fan_e_kwh"),

            round(coalesce(col("supply_air_flow").cast("float"), lit(0.0)), 2).alias("airflow_cmh"),
            round(coalesce(col("damper_position").cast("float"), lit(0.0)), 2).alias("damper_pct"),
            round(coalesce(col("temperature_setpoint").cast("float"), lit(0.0)), 2).alias("temp_sp_c"),
            round(coalesce(col("cooling_coil_valve_position").cast("float"), lit(0.0)), 2).alias("valve_pos_pct"),
            round(coalesce(col("ahu_fcu_fan_speed").cast("float"), lit(0.0)), 2).alias("fan_spd_hz"),
            round(coalesce(col("offcoil_air_temperature").cast("float"), lit(0.0)), 2).alias("offcoil_temp_c"),
            round(coalesce(col("offcoil_temperature_setpoint").cast("float"), lit(0.0)), 2).alias("offcoil_sp_c"),
            round(coalesce(col("supply_air_humidity").cast("float"), lit(0.0)), 2).alias("supply_hum_pct"),
            round(coalesce(col("pressure_across_filter").cast("float"), lit(0.0)), 2).alias("filter_press_pa"),
            round(coalesce(col("supply_air_static_pressure").cast("float"), lit(0.0)), 2).alias("static_press_pa"),
            round(coalesce(col("supply_air_temperature").cast("float"), lit(0.0)), 2).alias("supply_temp_c")
        )

    def start_processing(self, cleaned_df):

        print("---- Starting Categorized Stream Monitoring ----")

        ieq_df = cleaned_df.select(
            lit("Indoor Environmental").alias("Category"),
            "ts", "room", "voc_ppb", "co2_ppm", "temp_c",
            "hum_pct", "pm25_ugm3", "noise_dba", "lux"
        )

        energy_df = cleaned_df.select(
            lit("Energy").alias("Category"),
            "ts", "room", "fan_e_kwh", "light_e_kwh",
            "plug_e_kwh", "chw_e_kwh", "hvac_fan_e_kwh"
        )

        hvac_df = cleaned_df.select(
            lit("HVAC Operations").alias("Category"),
            "ts", "room", "airflow_cmh", "damper_pct",
            "temp_sp_c", "valve_pos_pct", "fan_spd_hz",
            "offcoil_temp_c", "offcoil_sp_c",
            "supply_hum_pct", "filter_press_pa",
            "static_press_pa", "supply_temp_c"
        )

        occ_df = cleaned_df.select(
            lit("Occupancy/WiFi").alias("Category"),
            "ts", "room", "wifi_dev_cnt", "occ_cnt"
        )

        ieq_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()

        energy_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()

        hvac_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()

        occ_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .option("truncate", "false") \
            .trigger(processingTime="10 seconds") \
            .start()

        return cleaned_df.writeStream \
            .format("parquet") \
            .option("path", self.hdfs_path) \
            .option("checkpointLocation", self.checkpoint_path) \
            .outputMode("append") \
            .start()


if __name__ == "__main__":

    KAFKA_BOOTSTRAP = "localhost:9092"
    KAFKA_TOPIC = "library_occupancy"

    processor = LibraryOccupancyProcessor()
    raw_stream = processor.read_from_kafka(KAFKA_BOOTSTRAP, KAFKA_TOPIC)
    cleaned_stream = processor.transform_data(raw_stream)

    hdfs_query = processor.start_processing(cleaned_stream)


    analytics = RBanalytics(cleaned_stream)

    occ_stream = analytics.occupancy_estimation()
    energy_stream = analytics.energy_usage_analysis()
    ieq_stream = analytics.ieq_monitoring()

    q1 = occ_stream.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("title", "Occupancy Analytics") \
        .start()

    q2 = energy_stream.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("title", "Energy Analytics") \
        .start()

    q3 = ieq_stream.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("title", "IEQ Analytics") \
        .start()

    print(f"Consumer started. Processing topic '{KAFKA_TOPIC}'...")

    processor.spark.streams.awaitAnyTermination()