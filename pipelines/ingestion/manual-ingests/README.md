# Manual DAta Ingest
This module is composed of many submodules which are ingesting data from CSV files.

## Submodule architecture
To give maxium flexibility to the developer, little constraints are imposed on this folder.

The submodules need to have a data folder, containing CSVs, and a cypher.py and ingest.py file.
- submodule
  - data
    - file1.csv
    - file2.csv
  - ingest.py
  - cypher.py 

## DAGs

Each submodule must be added to the Manual DAG: `dags/manual_data_ingests.py`.