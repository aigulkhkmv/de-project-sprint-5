import pendulum
from airflow.decorators import dag, task
from new_stg_load.api_data_loader import RawAPIDataLoader
from lib import ConnectionBuilder


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2023, 5, 12, tz="UTC"),
    catchup=False,
    tags=["origin2stg"],
    is_paused_upon_creation=True,
)
def stg_couriers_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="stg_courier_load")
    def load():
        courier_loader = RawAPIDataLoader(dwh_connect)
        courier_loader.load_couriers()

    load_dict = load()
    load_dict  #


stg_couriers_dag = stg_couriers_dag()
