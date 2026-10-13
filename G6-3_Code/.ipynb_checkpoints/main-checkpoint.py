# main.py
"""
Main pipeline runner for Library Occupancy Project.
This script runs all Python modules in order to process data,
compute analytics, environment summaries, comfort indices, energy usage, 
and export results.
"""

import os
from ingestion import RBproducer, RBconsumer
from processing import RBanalytics, RBbatch_processing
from utils import comfort_index, energy_usage, environment_control, space_optimization
from storage import export, MongoQuery, Neo4jQuery

def run_pipeline():
    print("\n=== 1️⃣ Data Ingestion Step ===")
    try:
        print("Running RBproducer...")
        RBproducer.main()  # assuming RBproducer has a main() function
        print("Running RBconsumer...")
        RBconsumer.main()  # assuming RBconsumer has a main() function
    except AttributeError:
        print("RBproducer/RBconsumer does not have main(), running as script instead.")
        os.system("python3 ingestion/RBproducer.py")
        os.system("python3 ingestion/RBconsumer.py")

    print("\n=== 2️⃣ Data Analytics Step ===")
    try:
        print("Running RBanalytics...")
        RBanalytics.main()  # or adjust function if different
    except AttributeError:
        os.system("python3 processing/RBanalytics.py")

    print("\n=== 3️⃣ Batch Processing Step ===")
    try:
        print("Running RBbatch_processing...")
        RBbatch_processing.main()  # or adjust if RBbatch_processing class-based
    except AttributeError:
        os.system("python3 processing/RBbatch_processing.py")

    print("\n=== 4️⃣ Compute Environment Metrics ===")
    try:
        print("Running comfort_index...")
        comfort_index.main()
    except AttributeError:
        os.system("python3 utils/comfort_index.py")

    try:
        print("Running environment_control...")
        environment_control.main()
    except AttributeError:
        os.system("python3 utils/environment_control.py")

    try:
        print("Running energy_usage...")
        energy_usage.main()
    except AttributeError:
        os.system("python3 utils/energy_usage.py")

    try:
        print("Running space_optimization...")
        space_optimization.main()
    except AttributeError:
        os.system("python3 utils/space_optimization.py")

    print("\n=== 5️⃣ Export to Databases ===")
    try:
        print("Running MongoQuery...")
        MongoQuery.main()
        print("Running Neo4jQuery...")
        Neo4jQuery.main()
        print("Running export.py...")
        export.main()
    except AttributeError:
        os.system("python3 storage/MongoQuery.py")
        os.system("python3 storage/Neo4jQuery.py")
        os.system("python3 storage/export.py")

    print("\nPipeline execution complete!")

if __name__ == "__main__":
    run_pipeline()