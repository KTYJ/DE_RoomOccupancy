from pyspark.sql import DataFrame
from pyspark.sql.functions import col, month, year, sum, round, lit, create_map
from pyspark.sql import functions as F

PRICE_PER_KWH = 0.79

MONTH_MAP = {
    1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
    7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"
}

def compute_avg_monthly_energy(df: DataFrame) -> DataFrame:
    """
    Compute average monthly energy consumption and cost across all rooms
    """

    df_monthly = df.withColumn("year", year(col("ts"))) \
                   .withColumn("month", month(col("ts")))

    monthly_energy = df_monthly.groupBy("year", "month").agg(
        round(sum("fan_e_kwh"), 2).alias("fan_kwh"),
        round(sum("light_e_kwh"), 2).alias("light_kwh"),
        round(sum("plug_e_kwh"), 2).alias("plug_kwh"),
        round(sum("chw_e_kwh"), 2).alias("chw_kwh")
    )

    monthly_energy = monthly_energy.withColumn("fan_price_per_kwh_RM", lit(PRICE_PER_KWH)) \
                                   .withColumn("light_price_per_kwh_RM", lit(PRICE_PER_KWH)) \
                                   .withColumn("plug_price_per_kwh_RM", lit(PRICE_PER_KWH)) \
                                   .withColumn("chw_price_per_kwh_RM", lit(PRICE_PER_KWH))

    monthly_energy = monthly_energy.withColumn("fan_total_RM", round(col("fan_kwh") * col("fan_price_per_kwh_RM"), 2)) \
                                   .withColumn("light_total_RM", round(col("light_kwh") * col("light_price_per_kwh_RM"), 2)) \
                                   .withColumn("plug_total_RM", round(col("plug_kwh") * col("plug_price_per_kwh_RM"), 2)) \
                                   .withColumn("chw_total_RM", round(col("chw_kwh") * col("chw_price_per_kwh_RM"), 2))

    month_map_expr = create_map([lit(x) for pair in MONTH_MAP.items() for x in pair])
    monthly_energy = monthly_energy.withColumn("month_name", month_map_expr[col("month")])

    monthly_energy = monthly_energy.select(
        "year",
        "month_name",
        "fan_kwh", "fan_price_per_kwh_RM", "fan_total_RM",
        "light_kwh", "light_price_per_kwh_RM", "light_total_RM",
        "plug_kwh", "plug_price_per_kwh_RM", "plug_total_RM",
        "chw_kwh", "chw_price_per_kwh_RM", "chw_total_RM"
    ).orderBy("year", "month")

    monthly_energy = monthly_energy.limit(12)

    return monthly_energy