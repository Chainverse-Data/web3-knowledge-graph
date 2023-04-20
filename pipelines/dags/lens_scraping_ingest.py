from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

dag = DAG(
    "lens_scraping_and_ingest",
    description="Scrapes the latest Lens data, and ingest them into the neo4J instance.",
    default_args={
        "start_date": datetime(2023, 4, 20),
        "owner": "Leo Blondel",
        "retries": 3
    },
    schedule_interval="@weekly",
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10080)
)

# Get ECS configuration from SSM parameters
ecs_cluster = Variable.get("MWAA_ECS_CLUSTER")  # str(ssm.get_parameter(Name='MWAA_ECS_CLUSTER', WithDecryption=True)['Parameter']['Value'])
ecs_subnets = Variable.get("MWAA_VPC_PRIVATE_SUBNETS") # str(ssm.get_parameter(Name='/mwaa/vpc/private_subnets', WithDecryption=True)['Parameter']['Value'])
ecs_security_group = Variable.get("MWAA_VPC_SECURITY_GROUPS") # str(ssm.get_parameter(Name='/mwaa/vpc/security_group', WithDecryption=True)['Parameter']['Value'])

# Choose the machine size from:
# pipelines-small: 1CPU 2Gb RAM
# pipelines-medium: 1CPU 8Gb RAM
# pipelines-large: 2CPU 16Gb RAM
# pipelines-xl: 8CPU 32Gb RAM
ecs_task_definition = "pipelines-xl"
ecs_task_image = "data-pipelines"
ecs_awslogs_group = f"/ecs/{ecs_task_definition}"
ecs_awslogs_stream_prefix = f"ecs/{ecs_task_image}"

# Get the container's ENV vars from Airflow Variables
env_vars = [
    {"name": "ETHERSCAN_API_KEY", "value": Variable.get("ETHERSCAN_API_KEY")},
    {"name": "ETHERSCAN_API_KEY_OPTIMISM", "value": Variable.get("ETHERSCAN_API_KEY_OPTIMISM")},
    {"name": "ETHERSCAN_API_KEY_POLYGON", "value": Variable.get("ETHERSCAN_API_KEY_POLYGON")},
    {"name": "ALCHEMY_API_KEY", "value": Variable.get("ALCHEMY_API_KEY")},
    {"name": "ALCHEMY_API_KEY_OPTIMISM", "value": Variable.get("ALCHEMY_API_KEY_OPTIMISM")},
    {"name": "ALCHEMY_API_KEY_ARBITRUM", "value": Variable.get("ALCHEMY_API_KEY_ARBITRUM")},
    {"name": "ALCHEMY_API_KEY_SOLANA", "value": Variable.get("ALCHEMY_API_KEY_SOLANA")},
    {"name": "ALCHEMY_API_KEY_POLYGON", "value": Variable.get("ALCHEMY_API_KEY_POLYGON")},
    {"name": "ALLOW_OVERRIDE", "value": Variable.get("ALLOW_OVERRIDE")},
    {"name": "AWS_BUCKET_PREFIX", "value": Variable.get("AWS_BUCKET_PREFIX")},
    {"name": "AWS_DEFAULT_REGION", "value": Variable.get("AWS_DEFAULT_REGION")},
    {"name": "AWS_ACCESS_KEY_ID", "value": Variable.get("AWS_ACCESS_KEY_ID")},
    {"name": "AWS_SECRET_ACCESS_KEY", "value": Variable.get("AWS_SECRET_ACCESS_KEY")},
    {"name": "LOGLEVEL", "value": Variable.get("LOGLEVEL")},
    {"name": "GRAPH_API_KEY", "value": Variable.get("GRAPH_API_KEY")},
    {"name": "REINITIALIZE", "value": Variable.get("REINITIALIZE")},
    {"name": "INGEST_FROM_DATE", "value": Variable.get("INGEST_FROM_DATE")},
    {"name": "INGEST_TO_DATE", "value": Variable.get("INGEST_TO_DATE")},
    {"name": "NEO_USERNAME", "value": Variable.get("NEO_USERNAME")},
    {"name": "NEO_URI", "value": Variable.get("NEO_URI")},
    {"name": "NEO_PASSWORD", "value": Variable.get("NEO_PASSWORD")},
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
lens_scrape_task = ECSOperator(
    task_id="lens_scraping",
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
                "command": ["python3", "-m", "pipelines.scraping.lens.scrape"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

lens_ingest_task = ECSOperator(
    task_id="lens_ingesting",
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
                "command": ["python3", "-m", "pipelines.ingestion.lens.ingest"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

lens_scrape_task >> lens_ingest_task