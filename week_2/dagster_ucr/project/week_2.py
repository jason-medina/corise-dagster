from typing import List

from dagster import In, Nothing, Out, ResourceDefinition, graph, op
from dagster_ucr.project.types import Aggregation, Stock
from dagster_ucr.resources import mock_s3_resource, redis_resource, s3_resource

"""
@op(
    config_schema={"s3_key": str},
    required_resource_keys={"s3"},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    stocks = context.resources.s3.get_data(context.op_config["s3_key"])
    return [Stock.from_list(stock) for stock in stocks]

@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Operation to output aggregate stock list"
)
def process_data(stocks: List[Stock]) -> Aggregation:
    stock_high = max(stocks, key=lambda x: x.high)
    return Aggregation(date=stock_high.date, high=stock_high.high)


@op(
    ins={"stock_high": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind": "redis"},
    description="Out with Stock agg to Redis"
)
def put_redis_data(context, stock_high):
    context.resources.redis.put_data(aggregations.date.strftime("%m/%d/%Y"), str(aggregations.high))
"""

@op(
    out={"stocks": Out(dagster_type=List[Stock])},
    required_resource_keys={"s3"},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    stocks = list()
    key = context.op_config["s3_key"]
    for row in context.resources.s3.get_data(key):
        stock = Stock.from_list(row)
        stocks.append(stock)
    return stocks


@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    tags={"kind": "transformation"},
    description="Find the largest stock value",
)
def process_data(stocks):
    big_stock = max(stocks, key=lambda x:x.high)
    return Aggregation(date=big_stock.date, high=big_stock.high)


@op(
    ins={"aggregations": In(dagster_type=Aggregation)},
    required_resource_keys={"redis"},
    tags={"kind":"redis"},
    description="Loads aggregations into redis cache",
)
def put_redis_data(context, aggregations):
    context.resources.redis.put_data(aggregations.date.strftime("%m/%d/%Y"), str(aggregations.high))

@graph
def week_2_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)
    pass


local = {
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

docker = {
    "resources": {
        "s3": {
            "config": {
                "bucket": "dagster",
                "access_key": "test",
                "secret_key": "test",
                "endpoint_url": "http://host.docker.internal:4566",
            }
        },
        "redis": {
            "config": {
                "host": "redis",
                "port": 6379,
            }
        },
    },
    "ops": {"get_s3_data": {"config": {"s3_key": "prefix/stock.csv"}}},
}

local_week_2_pipeline = week_2_pipeline.to_job(
    name="local_week_2_pipeline",
    config=local,
    resource_defs={"s3": mock_s3_resource, "redis": ResourceDefinition.mock_resource()},
)

docker_week_2_pipeline = week_2_pipeline.to_job(
    name="docker_week_2_pipeline",
    config=docker,
    resource_defs={"s3": s3_resource, "redis": redis_resource},
)
