====================================
Library Occupancy Analytics System
====================================

1. Project Description
This system analyzes library occupancy and environmental sensor data.

2. Technologies Used
- Kafka
- Spark
- Hadoop
- MongoDB
- Neo4j

3. How to Run
-----------------------------------------
3.0 Prior to beginning.. 

3.0.1 Run service
- start-dfs.sh
- start-yarn.sh
- zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties & 
- kafka-server-start.sh $KAFKA_HOME/config/server.properties &

3.0.2 Create directory
- Create directory based on following:

main folder: library_occupancy
subfolders: ingestion, processing, raw_data, storage, tests, utils

├── Occupancy_Estimation.csv
├── __pycache__
│   ├── RBanalytics.cpython-310.pyc
│   ├── comfort_index.cpython-310.pyc
│   ├── energy_usage.cpython-310.pyc
│   ├── environment_control.cpython-310.pyc
│   └── space_optimization.cpython-310.pyc
├── ingestion
│   ├── RBconsumer.py
│   └── RBproducer.py
├── processing
│   ├── RBanalytics.py
│   ├── RBbatch_processing.py
│   └── __pycache__
│       └── RBanalytics.cpython-310.pyc
├── raw_data
│   ├── combined_Room1.csv
│   ├── combined_Room2.csv
│   ├── combined_Room3.csv
│   ├── combined_Room4.csv
│   └── combined_Room5.csv
├── storage
│   ├── MongoQuery.py
│   ├── Neo4jQuery.py
│   └── export.py
├── tests
│   ├── test.py
│   ├── test2.py
│   └── test_mongo.py
└── utils
    ├── __pycache__
    │   ├── comfort_index.cpython-310.pyc
    │   ├── energy_usage.cpython-310.pyc
    │   ├── environment_control.cpython-310.pyc
    │   └── space_optimization.cpython-310.pyc
    ├── comfort_index.py
    ├── energy_usage.py
    ├── environment_control.py
    └── space_optimization.py

hduser@DESKTOP-1Q3GQFO: mkdir -p ~/library_occupancy
To navigate to correspond folder: cd ~/library_occupancy

3.0.3 Import data and scripts
- import .py files into ~/library_occupancy/
To check if data successfully imported:
hduser@DESKTOP-1Q3GQFO: ls ~/library_occupancy/raw_data (eg.)

3.0.4 Create Venv
Activate library_env
hduser@DESKTOP-1Q3GQFO: source library_env/bin/activate
hduser@DESKTOP-1Q3GQFO: export PYSPARK_DRIVER_PYTHON=jupyter

For Jupyter Lab:
hduser@DESKTOP-1Q3GQFO: export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0 --no-browser'
hduser@DESKTOP-1Q3GQFO: pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0

3.1 Run Producer 
hduser@DESKTOP-1Q3GQFO: kafka-console-consumer.sh --topic library_occupancy --from-beginning --bootstrap-server localhost:9092
hduser@DESKTOP-1Q3GQFO: python3 RBproducer.py

# Run these two lines in two different temrinal.

3.2 Run Consumer
hduser@DESKTOP-1Q3GQFO: spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
  RBconsumer.py

# Allow Consumer to run at least 15-20 min to ingest sufficient data

3.3 Run Batch Analysis
hduser@DESKTOP-1Q3GQFO: spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
  RBbatch_processing.py

------------------------------------------------------------------------------------------------
#Other Useful Command:

To exit venv:
hduser@DESKTOP-1Q3GQFO: Deactivate

To read data from hdfs
hduser@DESKTOP-1Q3GQFO: df = spark.read.parquet("/user/hduser/occupancy/robod_curated_v3/")

Print schema
hduser@DESKTOP-1Q3GQFO: df.printSchema()

View data
hduser@DESKTOP-1Q3GQFO: df.show(20, truncate=False)
hduser@DESKTOP-1Q3GQFO: df.show(df.count(), truncate=False)

Remove data
hduser@DESKTOP-1Q3GQFO: hdfs dfs -rm -r /user/hduser/occupancy/batch_output
