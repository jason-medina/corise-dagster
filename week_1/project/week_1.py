import csv
# import heapq ## github.com/set92/corise-dagster/blob/master/week_1/project/week_1.py
from datetime import datetime
from typing import List

from dagster import In, Nothing, Out, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: list):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )

def __lt__(self, other):
        return self.high < other.high


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


@op(
    config_schema={"s3_key": str},
    out={"stocks": Out(dagster_type=List[Stock])},
    tags={"kind": "s3"},
    description="Get a list of stocks from an S3 file",
)
def get_s3_data(context):
    output = list()
    with open(context.op_config["s3_key"]) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            stock = Stock.from_list(row)
            output.append(stock)
    return output

@op(
    ins={"stocks": In(dagster_type=List[Stock])},
    out={"aggregation": Out(dagster_type=Aggregation)},
    description="Operation to output aggregate stock list"
)
def process_data(stocks: List[Stock]) -> Aggregation:
# github.com/scottleechua/corise-dagster/blob/master/week_1/project/week_1.py    
    stock_high = max(stocks, key=lambda x :x.high)
    stock_agg = Aggregation(date=stock_high.date, high=stock_high.high)
    # max_stock = heapq.nlargest(1, stocks)[0]
    # return Aggregation(date=max_stock.date, high=max_stock.high)
    return stock_agg

@op(
    ins={"aggregation": In:(dagster_type=Aggregation)},
    description="Out with Stock agg to Redis"
)

@op(
    description = "Unload Aggregation to Redis",
    tags={"kind": "redis"})
def put_redis_data(aggregation: Aggregation) -> None:
    pass    


@job
def week_1_pipeline():
    data = process_data(get_s3_data())
    put_redis_data(data)
    pass
