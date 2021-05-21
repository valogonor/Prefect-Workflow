import csv
import datetime

from prefect import task, Flow, Parameter
from prefect.schedules import IntervalSchedule

@task(max_retries=5, retry_delay=datetime.timedelta(seconds=5))
def extract(path):
    with open(path, 'r') as f:
        text = f.readline().strip()
    data = [int(i) for i in text.split(',')]
    return data

@task
def transform(data):
    tdata = [i+1 for i in data]
    return tdata

@task
def load(data, path):
    with open(path, 'w') as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(data)
    return

def build_flow(schedule=None):
    with Flow('my_etl', schedule=schedule) as flow:
        path = Parameter(name='path', required=True)
        data = extract(path)
        tdata = transform(data)
        load(tdata, path)
    return flow

schedule = IntervalSchedule(
    start_date=datetime.datetime.now() + datetime.timedelta(seconds=1),
    interval=datetime.timedelta(seconds=5)
)

flow = build_flow(schedule)
flow.visualize()
flow.run(parameters={
    'path': 'values.csv'
})
