from typing import List

from dagster import (
    In,
    Nothing,
    Out,
    ResourceDefinition,
    RetryPolicy,
    RunRequest,
    ScheduleDefinition,
    SkipReason,
    graph,
    op,
    sensor,
    static_partitioned_config,
)
from project.resources import mock_s3_resource, redis_resource, s3_resource
from project.sensors import get_s3_keys
from project.types import Aggregation, Stock


@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock], description="List of Stocks")},
)
def get_s3_data(context):
    output = list()
    s3_key = context.op_config["s3_key"]
    for each in context.resources.s3.get_data(s3_key):
        stock = Stock.from_list(each)
        output.append(stock)

    return output


@op(ins={"stocks": In(dagster_type=List[Stock])},
    out={'aggregation': Out(dagster_type=Aggregation)},
    description="Given a list of stocks return the Aggregated values")
def process_data(stocks):
    highest = max([stock.high for stock in stocks])
    for stock in stocks:
        if stock.high == highest:
            agg = Aggregation(date=stock.date, high=highest)
    return agg


@op(ins={"aggregation": In(dagster_type=Aggregation)},
    out=Out(dagster_type=Nothing),
    required_resource_keys={"redis"},
    description='upload aggregated data to Redis')
def put_redis_data(context, aggregation):
    context.resources.redis.put_data(str(aggregation.date), str(aggregation.high))

@graph
def week_3_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}


docker_resource_config = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://localstack:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
}
docker = {
    **docker_resource_config,
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}},
}
# docker = docker_resource_config | {"ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock_9.csv"}}}}


PARTITIONS = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10"]


@static_partitioned_config(partition_keys=PARTITIONS)
def docker_config(partition_key: str):
    return {
        **docker_resource_config,
        "ops": {"get_s3_data": {"config": {"s3_key": f"prefix/stock_{partition_key}.csv"}}},
    }

local_week_3_pipeline = week_3_pipeline.to_job(
    name="local_week_3_pipeline",
    config=local,
    resource_defs={
        "s3": mock_s3_resource,
        "redis": ResourceDefinition.mock_resource(),
    },
)

docker_week_3_pipeline = week_3_pipeline.to_job(
    name="docker_week_3_pipeline",
    config=docker_config,
    resource_defs={
        "s3": s3_resource,
        "redis": redis_resource,
    },
    op_retry_policy=RetryPolicy(max_retries=10, delay=1),
)


local_week_3_schedule = ScheduleDefinition(job=local_week_3_pipeline, cron_schedule="*/15 * * * *")  # Add your schedule

docker_week_3_schedule = ScheduleDefinition(job=docker_week_3_pipeline, cron_schedule="0 * * * *")  # Add your schedule


@sensor(job=docker_week_3_pipeline, minimum_interval_seconds=1)
def docker_week_3_sensor(context):
    new_files = get_s3_keys(
        bucket="dagster",
        prefix="prefix"
    )
    if not new_files:
        yield SkipReason("No new s3 files found in bucket.")
        return
    for new_file in new_files:
        yield RunRequest(
            run_key=new_file,
            run_config={
                'resources': {'s3': {'config': {'bucket': 'dagster',
                                                 'access_key': 'test',
                                                 'secret_key': 'test',
                                                 'endpoint_url': 'http://localstack:4566'}
                                      },
                               'redis': {'config': {'host': 'redis',
                                                    'port': 6379}}},
                 'ops': {'get_s3_data': {'config': {'s3_key': new_file}}}}
        )