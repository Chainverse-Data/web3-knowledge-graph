# DAGS

This folder contains all the dags for the different pipelines.

# Machine size
You have to select a machine size for a task execution, these are set in the ECS AWS page, under task definitions. Each task definition has a set machine size. Currently those are available:
- `pipelines-small`: 1CPU 2Gb RAM
- `pipelines-medium`: 1CPU 8Gb RAM
- `pipelines-large`: 2CPU 16Gb RAM
- `pipelines-xl`: 8CPU 32Gb RAM
- `pipelines-huge`: 8CPU 64Gb RAM
- `pipelines-highcpu`: 16CPU 32Gb RAM

# Docker Image

# ECSOperator

# Network parameters
Make sure you add the network params and don't fucking touch them.

# Creating a DAG
