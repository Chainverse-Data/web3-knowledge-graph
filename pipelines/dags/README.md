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

Each task uses the docker image that is automatically built and deployed by the CI pipeline. This image is pushed to this address: `823557601923.dkr.ecr.us-east-2.amazonaws.com/chainverse/pipelines:latest`. This image is names: `data-pipelines` in the task and should be refered to as such in the ECSOperator. 

# Configuring a DAG

This is the meat of the DAGs. An ECSOperator task will trigger a deployement of a container (`data-pipelines`) into FARGATE, on a virtual machine, for execution. The machine automatically spins up, runs the container, then stops and removes itself. This is very efficient as ressources only exist when needed and no human interaction is required.

## DAG definition

This defines the DAG object. It contains AirFlow parameters such as the start_date the interval, title and description. Customize this according to the needs.

```python
dag = DAG(
    "DAG title",
    description="DAG Description.",
    default_args={
        "start_date": days_ago(2),
        "owner": "Owner name",
        "email": ["Owner Email"],
        "schedule_interval": "@daily", #@hourly, @daily, @weekly, @monthly
        "retries": 3 # Number of retries before it fails   
    },
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10080) # The timeout for the DAG in minutes, if it is a long DAG this needs to be a large number.
)
```

## Configuration
A lot of configuration needs to happen for this to work though. If you need to have different machine size per task, then redefine `ecs_task_definition` and `ecs_awslogs_group` before each task.

The only thing you need to setup is the `ecs_task_definition` according to the defintions above.

```python 
# This is the task definition name, see above for the list of available ones.
# THIS IS THE ONLY THING YOU SHOULD TOUCH NORMALLY
ecs_task_definition = "pipelines-highcpu"
# This is the task docker image name, it needs to be setup in the ECS task definition if a new one needs to be used. 
ecs_task_image = "data-pipelines"
# The next two var are setup automatically and are needed for logs to appear in AirFlow. 
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_image}"

# Get the container's ENV vars from Airflow Variables
# those need to be setup in advance in the Admin > Variables interface of AirFlow
env_vars = [
    {"name": "MY_VAR1", "value": Variable.get("MY_VAR1")},
    ...
    {"name": "MY_VARX", "value": Variable.get("MY_VARX")},
]

# DON'T CHANGE THIS! DON'T OMIT THIS! 
# Get ECS configuration from Airflow Variables. They need to be setup in AirFlow Admin > Variables so that the DAG can access them. 
ecs_cluster = Variable.get("MWAA_ECS_CLUSTER") 
ecs_subnets = Variable.get("MWAA_VPC_PRIVATE_SUBNETS") 
ecs_security_group = Variable.get("MWAA_VPC_SECURITY_GROUPS") 
# This is the internal AWS networking setup. If you touch it or omit it nothing will work. 
network_configuration={
    "awsvpcConfiguration": {
        "securityGroups": ecs_security_group.split(","),
        "subnets": ecs_subnets.split(","),
        "assignPublicIp": "ENABLED"
    },
}
```

## ECSOperator: Task definition

You need to change the names like `task_id` and the `example_ingest_task` to reflect the task action. 

Then you need to change the line `"command": ["python3", "-m", "pipelines.ingestion.ens.ingest"]`

This is a critical element, it will tell the docker image what action to run from the pipelines module. 
The only thing you need to change is normally the last bit `pipelines.ingestion.ens.ingest`. Change this to the actual action you want running according to this repo main README: `pipelines.[module_name].[service_name].[action]`

Also, make sure that all other vars remain as shown in this example.

```python
# This is an example ECSOperator task
example_ingest_task = ECSOperator(
    task_id="example_ingesting",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": ecs_task_image,
# This is the !!! IMPORTANT !!! part
# You need to set the command line that the docker will run
# This needs to be as list of args: ["cmd", "arg1", "arg2"]
                "command": ["python3", "-m", "pipelines.ingestion.ens.ingest"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)
```

# Creating a DAG

Then the flow to create a DAG is fairly straightforward. You first define the configurations as mentionned above, then you create a number of ECSOperator tasks, and finally you chain them using Airflow `>>` chain operator.

The easiest way to NOT FUCK UP, is to copy an existing DAG and to modify the dag metadata, tasks commands and task_definition. 

Here is an example:
```python
from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

dag = DAG(
    "example_dag",
    description="Just an example.",
    default_args={
        "start_date": days_ago(2),
        "owner": "Leo Blondel",
        "email": ["leo@blondel.ninja"],
        "schedule_interval": "@daily",
        "retries": 3        
    },
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10080)
)

# Get ECS configuration from SSM parameters
ecs_cluster = Variable.get("MWAA_ECS_CLUSTER")
ecs_subnets = Variable.get("MWAA_VPC_PRIVATE_SUBNETS")
ecs_security_group = Variable.get("MWAA_VPC_SECURITY_GROUPS")

# Choose the machine size for the pipeline, if you need to change this for some of the jobs redfine them before each job.
ecs_task_definition = "pipelines-medium"
ecs_task_image = "data-pipelines"
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_image}"

# Get the container's ENV vars from Airflow Variables
env_vars = [
    {"name": "ENV_1", "value": Variable.get("ENV_1")},
    ...
    {"name": "ENV_X", "value": Variable.get("ENV_X")},
]

network_configuration={
    "awsvpcConfiguration": {
        "securityGroups": ecs_security_group.split(","),
        "subnets": ecs_subnets.split(","),
        "assignPublicIp": "ENABLED"
    },
}

    
# Run Docker container via ECS operator
# There are two fields set here:
# task_id to give it a name
# overrides -> command -> to set it to the module command 
task_1 = ECSOperator(
    task_id="task_1",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.module.service.action"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

task_2 = ECSOperator(
    task_id="task_2",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.module.service.action"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

task_1 >> task_2
```