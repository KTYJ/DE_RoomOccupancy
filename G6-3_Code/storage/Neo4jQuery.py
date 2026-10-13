from pyspark.sql import SparkSession
from neo4j import GraphDatabase
import os


class LibraryNeo4jExporter:
    """
    Exports library sensor data from HDFS (Parquet) to Neo4j
    and runs graph analytics queries.
    """

    BASE_PATH = "/user/hduser/occupancy/batch_output/"

    TABLES = [
        "Room_1_sensor",
        "Room_2_sensor",
        "Room_3_sensor",
        "Room_4_sensor",
        "Room_5_sensor",
    ]

    QUERIES = [
        (
            "Average Temperature Per Room",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   AVG(reading.temp) AS avg_temperature
            ORDER BY avg_temperature
            """,
        ),
        (
            "Average CO2 Per Room",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   AVG(reading.co2) AS avg_co2
            ORDER BY avg_co2
            """,
        ),
        (
            "Most Occupied Room",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   SUM(reading.occupancy) AS total_people
            ORDER BY total_people DESC
            LIMIT 1
            """,
        ),
        (
            "Quietest Room",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   AVG(reading.noise) AS avg_noise
            ORDER BY avg_noise ASC
            LIMIT 3
            """,
        ),
        (
            "Brightest Room",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   AVG(reading.lux) AS avg_light
            ORDER BY avg_light DESC
            LIMIT 3
            """,
        ),
        (
            "Highest Energy Consumption",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   SUM(reading.fan_energy + reading.light_energy + reading.plug_energy) AS total_energy
            ORDER BY total_energy DESC
            LIMIT 3
            """,
        ),
        (
            "Best Study Environment",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            WHERE reading.occupancy > 0
            RETURN r.name AS room,
                   AVG(reading.temp) AS avg_temp,
                   AVG(reading.noise) AS avg_noise,
                   AVG(reading.co2) AS avg_co2
            ORDER BY avg_noise ASC
            LIMIT 5
            """,
        ),
        (
            "Air Pollution Levels",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            RETURN r.name AS room,
                   AVG(reading.pm25) AS avg_pm25
            ORDER BY avg_pm25 DESC
            LIMIT 5
            """,
        ),
        (
            "Peak Occupancy",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            WITH date(reading.start_time) AS day, reading.occupancy AS occ
            RETURN day, ceil(SUM(occ)) AS total_people
            ORDER BY total_people DESC
            LIMIT 5
            """,
        ),
        (
            "Room Comfort Score",
            """
            MATCH (r:Room)-[:HAS_READING]->(reading)
            WITH r.name AS room,
                 AVG(reading.temp) AS temp,
                 AVG(reading.noise) AS noise,
                 AVG(reading.co2) AS co2
            WITH room,
                 (100 - abs(temp - 25)*4 - noise*0.3 - co2*0.01) AS comfort_score
            RETURN room, comfort_score
            ORDER BY comfort_score DESC
            LIMIT 5
            """,
        ),
    ]

    # ------------------------------------------------------------------ #

    def __init__(self, neo4j_uri: str, neo4j_user: str, neo4j_password: str):
        self.neo4j_uri = neo4j_uri
        self.neo4j_user = neo4j_user
        self.neo4j_password = neo4j_password

        self.spark = self._create_spark_session()
        self.driver = GraphDatabase.driver(
            self.neo4j_uri,
            auth=(self.neo4j_user, self.neo4j_password),
        )

    # ------------------------------------------------------------------ #
    #  Spark                                                               #
    # ------------------------------------------------------------------ #

    def _create_spark_session(self) -> SparkSession:
        spark = (
            SparkSession.builder
            .appName("Library_Export_to_Neo4j")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark

    # ------------------------------------------------------------------ #
    #  Neo4j helpers                                                       #
    # ------------------------------------------------------------------ #

    def clear_previous_readings(self) -> None:
        """Delete all existing Reading nodes and their relationships."""
        with self.driver.session() as session:
            session.run(
                "MATCH (r:Room)-[h:HAS_READING]->(reading) DETACH DELETE reading"
            )
        print("✅ Previous readings deleted")

    @staticmethod
    def _insert_batch(tx, rows: list[dict]) -> None:
        query = """
        UNWIND $rows AS row

        MERGE (r:Room {name: row.room})

        CREATE (reading:Reading {
            start_time:       row.start_time,
            end_time:         row.end_time,
            voc:              row.voc_ppb,
            co2:              row.co2_ppm,
            temp:             row.temp_c,
            humidity:         row.hum_pct,
            pm25:             row.pm25_ugm3,
            noise:            row.noise_dba,
            lux:              row.lux,
            wifi_dev_cnt:     row.wifi_dev_cnt,
            occupancy:        row.occ_cnt,
            fan_energy:       row.fan_e_kwh,
            light_energy:     row.light_e_kwh,
            plug_energy:      row.plug_e_kwh,
            chw_energy:       row.chw_e_kwh,
            airflow:          row.airflow_cmh,
            damper_pct:       row.damper_pct,
            valve_pct:        row.valve_pos_pct,
            supply_humidity:  row.supply_hum_pct,
            filter_pressure:  row.filter_press_pa,
            supply_temp:      row.supply_temp_c
        })

        MERGE (r)-[:HAS_READING]->(reading)
        """
        tx.run(query, rows=rows)

    @staticmethod
    def _run_query(tx, query: str, title: str) -> None:
        print(f"\n===== {title} =====")
        for record in tx.run(query):
            print(record)

    # ------------------------------------------------------------------ #
    #  Export                                                              #
    # ------------------------------------------------------------------ #

    def export_table(self, table: str) -> None:
        """Read one Parquet table from HDFS and write it to Neo4j."""
        path = os.path.join(self.BASE_PATH, table)
        print(f"\nReading: {path}")

        pd_df = self.spark.read.parquet(path).toPandas()

        if pd_df.empty:
            print(f"Skipped (empty table): {table}")
            return

        rows = pd_df.to_dict("records")

        with self.driver.session() as session:
            session.execute_write(self._insert_batch, rows)

        print(f"Inserted {len(rows)} rows from {table}")

    def export_all_tables(self) -> None:
        """Export every table listed in TABLES."""
        for table in self.TABLES:
            self.export_table(table)

    # ------------------------------------------------------------------ #
    #  Analytics                                                           #
    # ------------------------------------------------------------------ #

    def run_analytics(self) -> None:
        """Execute all predefined Cypher analytics queries."""
        print("\nRunning Graph Analytics Queries...\n")
        with self.driver.session() as session:
            for title, query in self.QUERIES:
                session.execute_read(self._run_query, query, title)

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Shut down Spark and the Neo4j driver cleanly."""
        self.spark.stop()
        self.driver.close()
        print("\n✅ All Parquet tables exported and queries executed successfully!")

    def run(self) -> None:
        """Full pipeline: clear → export → analyse → close."""
        try:
            self.clear_previous_readings()
            self.export_all_tables()
            self.run_analytics()
        finally:
            self.close()


# ------------------------------------------------------------------ #
#  Entry point                                                         #
# ------------------------------------------------------------------ #

if __name__ == "__main__":
    exporter = LibraryNeo4jExporter(
        neo4j_uri="neo4j+s://68cd1cee.databases.neo4j.io",
        neo4j_user="68cd1cee",
        neo4j_password="dQDMCR-jdPb7l8UVRGB3_wG7McFR0v5HG28z24ONYGo",
    )
    exporter.run()