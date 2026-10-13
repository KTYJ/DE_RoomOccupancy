from pyspark.sql.functions import window, avg, sum, max, col, round

class RBanalytics:

    def __init__(self, cleaned_df):
        self.df = cleaned_df

    def occupancy_estimation(self):
        return self.df.groupBy(
            window(col("ts"), "1 hour"),
            col("room")
        ).agg(
            round(avg("wifi_dev_cnt"), 2).alias("avg_wifi_devices"),
            round(avg("co2_ppm"), 2).alias("avg_co2_ppm"),
            round(avg("noise_dba"), 2).alias("avg_noise_dba"),
            round(max("wifi_dev_cnt"), 2).alias("peak_wifi_devices")
        ).select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            "room", "avg_wifi_devices", "avg_co2_ppm", "avg_noise_dba", "peak_wifi_devices"
        )

    def energy_usage_analysis(self):
        return self.df.groupBy(
            window(col("ts"), "1 hour"),
            col("room")
        ).agg(
            round(sum("fan_e_kwh"), 2).alias("total_fan_energy"),
            round(sum("light_e_kwh"), 2).alias("total_light_energy"),
            round(sum("plug_e_kwh"), 2).alias("total_plug_energy"),
            round(sum("chw_e_kwh"), 2).alias("total_chilled_water_energy"),
            round(sum("hvac_fan_e_kwh"), 2).alias("total_hvac_fan_energy")
        ).select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            "room", "total_fan_energy", "total_light_energy", "total_plug_energy", 
            "total_chilled_water_energy", "total_hvac_fan_energy"
        )

    def ieq_monitoring(self):
        return self.df.groupBy(
            window(col("ts"), "1 hour"),
            col("room")
        ).agg(
            round(avg("temp_c"), 2).alias("avg_temperature"),
            round(avg("hum_pct"), 2).alias("avg_humidity"),
            round(avg("pm25_ugm3"), 2).alias("avg_pm25"),
            round(avg("voc_ppb"), 2).alias("avg_voc"),
            round(avg("co2_ppm"), 2).alias("avg_co2")
        ).select(
            col("window.start").alias("hour_start"),
            col("window.end").alias("hour_end"),
            "room", "avg_temperature", "avg_humidity", "avg_pm25", "avg_voc", "avg_co2"
        )