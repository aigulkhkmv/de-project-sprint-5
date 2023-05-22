import os
import sys

import pendulum
from airflow.decorators import dag, task
from stg2dds.data_loader import StgDataLoader

sys.path.append(os.path.realpath('../'))
from connector import ConnectionBuilder

@dag(
    schedule_interval="0/60 * * * *",
    start_date=pendulum.datetime(2023, 5, 12, tz="UTC"),
    catchup=False,
    tags=["origin2stg"],
    is_paused_upon_creation=True,
)
def dds_couriers_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dds_courier_load")
    def load():
        courier_loader = StgDataLoader(dwh_connect)
        courier_loader.load_couriers()

    load_dict = load()
    load_dict  #


dds_couriers_dag = dds_couriers_dag()
