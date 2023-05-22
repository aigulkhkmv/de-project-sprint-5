import os
import sys

import pendulum
from airflow.decorators import dag, task

sys.path.append(os.path.realpath('../'))
from stg2dds.data_loader import StgDataLoader
from connector import ConnectionBuilder

@dag(
    schedule_interval="0/60 * * * *",
    start_date=pendulum.datetime(2023, 5, 12, tz="UTC"),
    catchup=False,
    tags=["origin2stg"],
    is_paused_upon_creation=True,
)
def dds_delivery_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dds_deliveries_load")
    def load():
        delivery_loader = StgDataLoader(dwh_connect)
        delivery_loader.load_deliveries()

    load_dict = load()
    load_dict  #


dds_delivery_dag = dds_delivery_dag()
