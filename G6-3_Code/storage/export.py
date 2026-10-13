from pyspark.sql import SparkSession
from pymongo import MongoClient
import subprocess
import certifi

class HdfsToMongoExporter:
    """
    Class to export HDFS Parquet tables to MongoDB Atlas.
    """

    def __init__(self, spark_app_name, base_hdfs_path, mongo_uri, db_name):
        # Spark Session
        self.spark = SparkSession.builder.appName(spark_app_name).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")

        # HDFS path
        self.base_hdfs_path = base_hdfs_path

        # MongoDB connection
        self.client = MongoClient(mongo_uri, tlsCAFile=certifi.where())
        self.db = self.client[db_name]

    def list_hdfs_dirs(self):
        """List all subdirectories in HDFS base path"""
        cmd = ["hdfs", "dfs", "-ls", self.base_hdfs_path]
        result = subprocess.run(cmd, capture_output=True, text=True)
        dirs = []
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) == 8 and parts[0].startswith('d'):
                dirs.append(parts[-1].rstrip('/').split('/')[-1])
        return dirs

    def export_parquet_to_mongo(self, collection_name, parquet_subdir):
        """Export a single HDFS Parquet table to MongoDB"""
        hdfs_path = f"{self.base_hdfs_path}{parquet_subdir}"
        print(f"\nReading Parquet from: {hdfs_path}")

        try:
            df_spark = self.spark.read.parquet(hdfs_path)
        except Exception as e:
            print(f"[Error] Failed to read {parquet_subdir}: {e}")
            return

        df_pd = df_spark.toPandas()
        if df_pd.empty:
            print(f"[Skipped] {collection_name} is empty.")
            return

        if collection_name in self.db.list_collection_names():
            self.db[collection_name].drop()

        self.db[collection_name].insert_many(df_pd.to_dict("records"))
        print(f"[Success] {collection_name} exported to MongoDB ({len(df_pd)} records).")

    def export_all_tables(self):
        """Export all HDFS subdirectories to MongoDB"""
        all_tables = self.list_hdfs_dirs()
        print(f"Found HDFS folders: {all_tables}")

        for table_name in all_tables:
            self.export_parquet_to_mongo(table_name, table_name)

    def close_connections(self):
        """Close Spark and MongoDB connections"""
        self.spark.stop()
        self.client.close()


# ----------------------------
# Main execution
# ----------------------------
if __name__ == "__main__":
    MONGO_URI = "mongodb+srv://klwongwm21_db_user:1RtBdiUtFojf2GMf@wongkahlok.rbyed6t.mongodb.net/?appName=WongKahLok"
    exporter = HdfsToMongoExporter(
        spark_app_name="Library_Export_to_MongoDB",
        base_hdfs_path="/user/hduser/occupancy/batch_output/",
        mongo_uri=MONGO_URI,
        db_name="library"
    )

    exporter.export_all_tables()
    exporter.close_connections()

    print("\n✅ All HDFS Parquet tables successfully exported to MongoDB Atlas!")