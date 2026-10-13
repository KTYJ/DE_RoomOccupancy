from pyspark.sql.functions import avg, min, max, when
from pyspark.sql import Row


def compute_environment_summary(df, spark):
    """
    Generate a General Environmental Summary Table.
    """

    # ---------------------------------------------------------
    # 1. Compute Statistics
    # ---------------------------------------------------------
    stats = df.select(
        avg("temp_c").alias("temp_avg"),
        min("temp_c").alias("temp_min"),
        max("temp_c").alias("temp_max"),

        avg("hum_pct").alias("hum_avg"),
        min("hum_pct").alias("hum_min"),
        max("hum_pct").alias("hum_max"),

        avg("co2_ppm").alias("co2_avg"),
        min("co2_ppm").alias("co2_min"),
        max("co2_ppm").alias("co2_max"),

        avg("pm25_ugm3").alias("pm_avg"),
        min("pm25_ugm3").alias("pm_min"),
        max("pm25_ugm3").alias("pm_max"),

        avg("noise_dba").alias("noise_avg"),
        min("noise_dba").alias("noise_min"),
        max("noise_dba").alias("noise_max"),

        avg("lux").alias("light_avg"),
        min("lux").alias("light_min"),
        max("lux").alias("light_max")
    ).collect()[0]

    # ---------------------------------------------------------
    # 2. Create Summary Rows
    # ---------------------------------------------------------
    rows = [

        Row(
            Metric="Temperature (°C)",
            Avg=round(stats["temp_avg"], 2),
            Min=round(stats["temp_min"], 2),
            Max=round(stats["temp_max"], 2),
            Recommended_Range="18–30",
            Status="Normal" if 18 <= stats["temp_avg"] <= 30 else "Out of Range"
        ),

        Row(
            Metric="Humidity (%)",
            Avg=round(stats["hum_avg"], 2),
            Min=round(stats["hum_min"], 2),
            Max=round(stats["hum_max"], 2),
            Recommended_Range="30–70",
            Status="Normal" if 30 <= stats["hum_avg"] <= 70 else "Out of Range"
        ),

        Row(
            Metric="CO2 (ppm)",
            Avg=round(stats["co2_avg"], 2),
            Min=round(stats["co2_min"], 2),
            Max=round(stats["co2_max"], 2),
            Recommended_Range="<1000",
            Status="Good" if stats["co2_avg"] < 800 else
                   "Moderate" if stats["co2_avg"] <= 1000 else
                   "High"
        ),

        Row(
            Metric="PM2.5 (µg/m³)",
            Avg=round(stats["pm_avg"], 2),
            Min=round(stats["pm_min"], 2),
            Max=round(stats["pm_max"], 2),
            Recommended_Range="<50",
            Status="Good" if stats["pm_avg"] < 25 else
                   "Moderate" if stats["pm_avg"] <= 50 else
                   "Unhealthy"
        ),

        Row(
            Metric="Noise (dBA)",
            Avg=round(stats["noise_avg"], 2),
            Min=round(stats["noise_min"], 2),
            Max=round(stats["noise_max"], 2),
            Recommended_Range="30–80",
            Status="Normal" if 30 <= stats["noise_avg"] <= 80 else "Too Loud"
        ),

        Row(
            Metric="Light (lux)",
            Avg=round(stats["light_avg"], 2),
            Min=round(stats["light_min"], 2),
            Max=round(stats["light_max"], 2),
            Recommended_Range="300–1000",
            Status="Good" if 300 <= stats["light_avg"] <= 1000 else "Poor"
        )

    ]

    summary_df = spark.createDataFrame(rows)

    return summary_df
