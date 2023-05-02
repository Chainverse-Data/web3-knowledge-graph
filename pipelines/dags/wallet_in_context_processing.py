from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.ecs_operator import ECSOperator
from airflow.models import Variable

dag = DAG(
    "wallet_in_context_processing",
    description="Runs the Wallet In Context processing in succession.",
    default_args={
        "start_date": datetime(2023, 4, 20),
        "owner": "Leo Blondel",
        "retries": 3
    },
    schedule_interval= "@daily",
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
ecs_task_definition_processing = "pipelines-small"
ecs_awslogs_group_processing = f"/ecs/{ecs_task_definition_processing}"

ecs_task_image = "data-pipelines"
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
wic_incentive_farming = ECSOperator(
    task_id="wic_incentive_farming",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.farmers.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_creators = ECSOperator(
    task_id="wic_creators",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.creators.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_collectors = ECSOperator(
    task_id="wic_collectors",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.collectors.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_traders = ECSOperator(
    task_id="wic_traders",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.traders.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_developers = ECSOperator(
    task_id="wic_developers",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.developers.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_public_goods = ECSOperator(
    task_id="wic_public_goods",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.publicGoods.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_protocol_politicians = ECSOperator(
    task_id="wic_protocol_politicians",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.protocolPoliticians.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_professionals = ECSOperator(
    task_id="wic_professionals",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.professionals.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_influencers = ECSOperator(
    task_id="wic_influencers",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.influencers.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_dao_contributors = ECSOperator(
    task_id="wic_dao_contributors",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wic.daoContributors.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

audiences_post_processing_task = ECSOperator(
    task_id="audiences_post_processing",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.postProcessing.audiences.process"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wicScores_post_processing_task = ECSOperator(
    task_id="wicScores_post_processing_task",
    dag=dag,
    aws_conn_id="aws_ecs",
    cluster=ecs_cluster,
    task_definition=ecs_task_definition_processing,
    region_name="us-east-2",
    launch_type="FARGATE",
    overrides={
        "containerOverrides": [
            {
                "name": "data-pipelines",
                "command": ["python3", "-m", "pipelines.analytics.wicScore.analyze"],
                "environment": env_vars
            },
        ],
    },
    network_configuration=network_configuration,
    awslogs_group=ecs_awslogs_group_processing,
    awslogs_stream_prefix=ecs_awslogs_stream_prefix
)

wic_incentive_farming >> [wic_creators, wic_collectors, wic_traders, wic_developers, wic_protocol_politicians, wic_professionals, wic_influencers, wic_public_goods, wic_dao_contributors] >> wicScores_post_processing_task