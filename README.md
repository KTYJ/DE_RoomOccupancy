# Library Occupancy Analytics System

The Library Occupancy Analytics System is a data-driven solution designed to monitor and analyze how library spaces are being used in real time. The platform combines environmental sensor data and occupancy patterns to provide insights into room usage, comfort, energy efficiency, and space optimization.

This project supports evidence-based decision-making for facility managers, library administrators, and operations teams by transforming raw sensor readings into actionable analytics.

## Business Value
The system helps stakeholders:

- Understand room occupancy trends across different library spaces
- Identify underused or overcrowded areas
- Monitor environmental comfort indicators such as temperature, humidity, and air quality
- Improve energy efficiency by analyzing usage patterns
- Support optimized space planning and operational management
- Enable data-driven recommendations for library layout and resource allocation

## Architecture and Technology Stack
The solution integrates several modern data processing technologies:

- Kafka for streaming data ingestion
- Spark for large-scale batch and stream processing
- Hadoop for distributed storage and data processing support
- MongoDB for document-based storage
- Neo4j for graph-based relationship analysis

## Data Processing Flow
1. Sensor and occupancy data are collected and prepared in the `raw_data` folder.
2. Kafka producers publish the incoming data stream.
3. Spark-based consumers process and transform the stream.
4. Analytics jobs generate room-level and aggregate insights.
5. Results are exported to storage systems and analytical outputs for further review.

## Key Functional Areas
- Ingestion: data stream collection and publishing
- Processing: analytics and transformation logic
- Storage: export and query support for MongoDB and Neo4j
- Utilities: reusable analytical modules for comfort, energy, control, and optimization
- Raw Data: input datasets for rooms 1 through 5

## Project Structure
```text
library_occupancy/
├── ingestion/
│   ├── RBconsumer.py
│   └── RBproducer.py
├── processing/
│   ├── RBanalytics.py
│   ├── RBbatch_processing.py
│   └── __pycache__/
├── raw_data/
│   ├── combined_Room1.csv
│   ├── combined_Room2.csv
│   ├── combined_Room3.csv
│   ├── combined_Room4.csv
│   └── combined_Room5.csv
├── storage/
│   ├── MongoQuery.py
│   ├── Neo4jQuery.py
│   └── export.py
├── utils/
│   ├── comfort_index.py
│   ├── energy_usage.py
│   ├── environment_control.py
│   └── space_optimization.py
├── tests/
│   ├── test.py
│   ├── test2.py
│   └── test_mongo.py
└── README.md
```

## Prerequisites
Before running the system, ensure the following are available:

- Hadoop services are running
- Kafka and ZooKeeper services are running
- Spark is installed and configured
- Python environment is prepared
- Required Python libraries and Spark packages are available

## Security Note
The MongoDB connection string used in the project configuration is intentionally masked or truncated in the repository to protect sensitive credentials. Do not commit real database URIs, passwords, or connection secrets to version control.

Before running the export workflow in a real environment, replace the placeholder value with a valid connection string stored securely in environment variables or a protected configuration file.

## Setup Steps
### 1. Start the required services
```bash
start-dfs.sh
start-yarn.sh
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
kafka-server-start.sh $KAFKA_HOME/config/server.properties &
```

### 2. Create the project directory
```bash
mkdir -p ~/library_occupancy
cd ~/library_occupancy
```

### 3. Import project files and datasets
Import the Python files and datasets into the project directory. Confirm that the data is available in the `raw_data` folder.

### 4. Create and activate a virtual environment
```bash
source library_env/bin/activate
export PYSPARK_DRIVER_PYTHON=jupyter
```

For Jupyter Lab:
```bash
export PYSPARK_DRIVER_PYTHON_OPTS='lab --ip=0.0.0.0 --no-browser'
pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
```

## Execution Workflow
### Run Producer
```bash
kafka-console-consumer.sh --topic library_occupancy --from-beginning --bootstrap-server localhost:9092
python3 RBproducer.py
```

> Run these commands in separate terminal sessions.

### Run Consumer
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
  RBconsumer.py
```

The consumer should be allowed to run for at least 15–20 minutes to ingest a sufficient volume of data.

### Run Batch Analysis
```bash
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 \
  RBbatch_processing.py
```

## Outputs and Analytics
The analytics pipeline produces insights such as:

- Daily and hourly usage trends
- Monthly energy usage averages
- Environmental comfort index calculations
- Room-specific occupancy patterns
- Space optimization recommendations
- Environment control insights

## Useful Commands
### Exit virtual environment
```bash
deactivate
```

### Read Parquet data from HDFS
```python
df = spark.read.parquet("/user/hduser/occupancy/robod_curated_v3/")
```

### Print schema
```python
df.printSchema()
```

### View data
```python
df.show(20, truncate=False)
df.show(df.count(), truncate=False)
```

### Remove generated output
```bash
hdfs dfs -rm -r /user/hduser/occupancy/batch_output
```

## Summary
The Library Occupancy Analytics System provides a scalable, analytics-driven foundation for understanding library usage and environmental conditions. It turns large volumes of sensor and occupancy data into practical operational intelligence, helping improve both user experience and facility efficiency.

This project is especially valuable for organizations seeking to improve space utilization, optimize infrastructure operations, and support data-informed resource planning.
