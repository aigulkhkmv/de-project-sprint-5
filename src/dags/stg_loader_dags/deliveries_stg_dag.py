import json
import os
import sys
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pendulum
import psycopg
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from bson.objectid import ObjectId
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from requests import get

sys.path.append(os.path.realpath("../"))
from connector import ConnectionBuilder, PgConnect


def json2str(obj: Any) -> str:
    return json.dumps(to_dict(obj), sort_keys=True, ensure_ascii=False)


def str2json(str: str) -> Dict:
    return json.loads(str)


def to_dict(obj, classkey=None):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    elif isinstance(obj, ObjectId):
        return str(obj)
    if isinstance(obj, dict):
        data = {}
        for (k, v) in obj.items():
            data[k] = to_dict(v, classkey)
        return data
    elif hasattr(obj, "_ast"):
        return to_dict(obj._ast())
    elif hasattr(obj, "__iter__") and not isinstance(obj, str):
        return [to_dict(v, classkey) for v in obj]
    elif hasattr(obj, "__dict__"):
        data = dict(
            [
                (key, to_dict(value, classkey))
                for key, value in obj.__dict__.items()
                if not callable(value) and not key.startswith("_")
            ]
        )
        if classkey is not None and hasattr(obj, "__class__"):
            data[classkey] = obj.__class__.__name__
        return data
    else:
        return obj


class PgConnect:
    def __init__(
        self,
        host: str,
        port: str,
        db_name: str,
        user: str,
        pw: str,
        sslmode: str = "require",
    ) -> None:
        self.host = host
        self.port = int(port)
        self.db_name = db_name
        self.user = user
        self.pw = pw
        self.sslmode = sslmode

    def url(self) -> str:
        return """
            host={host}
            port={port}
            dbname={db_name}
            user={user}
            password={pw}
            target_session_attrs=read-write
            sslmode={sslmode}
        """.format(
            host=self.host,
            port=self.port,
            db_name=self.db_name,
            user=self.user,
            pw=self.pw,
            sslmode=self.sslmode,
        )

    def client(self):
        return psycopg.connect(self.url())

    @contextmanager
    def connection(self) -> Generator[psycopg.Connection, None, None]:
        conn = psycopg.connect(self.url())
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()


class ConnectionBuilder:
    @staticmethod
    def pg_conn(conn_id: str) -> PgConnect:
        conn = BaseHook.get_connection(conn_id)

        sslmode = "require"
        if "sslmode" in conn.extra_dejson:
            sslmode = conn.extra_dejson["sslmode"]

        pg = PgConnect(
            str(conn.host),
            str(conn.port),
            str(conn.schema),
            str(conn.login),
            str(conn.password),
            sslmode,
        )

        return pg


class RawCourier(BaseModel):
    courier_id: str
    object_value: str


class RawDelivery(BaseModel):
    delivery_id: str
    object_value: str


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class StgEtlSettingsRepository:
    def get_setting(self, conn: Connection, etl_key: str) -> Optional[EtlSetting]:
        with conn.cursor(row_factory=class_row(EtlSetting)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        workflow_key,
                        workflow_settings
                    FROM stg.srv_wf_settings
                    WHERE workflow_key = %(etl_key)s;
                """,
                {"etl_key": etl_key},
            )
            obj = cur.fetchone()

        return obj

    def save_setting(
        self, conn: Connection, workflow_key: str, workflow_settings: str
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.srv_wf_settings(workflow_key, workflow_settings)
                    VALUES (%(etl_key)s, %(etl_setting)s)
                    ON CONFLICT (workflow_key) DO UPDATE
                    SET workflow_settings = EXCLUDED.workflow_settings;
                """,
                {"etl_key": workflow_key, "etl_setting": workflow_settings},
            )


class RawAPIDataLoader:
    def __init__(self, stg: PgConnect) -> None:
        self.stg = stg
        self.settings_repository = StgEtlSettingsRepository()
        self.wf_key_couriers = "couriers_origin_to_stg_workflow"
        self.wf_key_deliveries = "deliveries_origin_to_stg_workflow"
        self.api_nickname = "aigulkhkmv"
        self.api_cohort = "12"
        self.api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
        self.api_header = {
            "X-Nickname": self.api_nickname,
            "X-Cohort": self.api_cohort,
            "X-API-KEY": self.api_key,
        }

    def get_deliveries(self, offset: int) -> List[Dict]:
        print("Get delivery data by API")
        response = get(
            "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=id&sort_direction=asc&limit=50"
            "&offset={}".format(offset),
            headers=self.api_header,
        )
        return response.json()

    def get_couriers(self, offset: int) -> List[Dict]:
        print("Get couriers data by API")
        response = get(
            "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=id&sort_direction=asc&limit=50"
            "&offset={}".format(offset),
            headers=self.api_header,
        )
        return response.json()

    def insert_couriers(self, conn, raw_courier: RawCourier) -> None:
        with conn.cursor() as cur:
            print("Insert couriers data")
            cur.execute(
                """
                    INSERT INTO stg.couriers(courier_id, object_value)
                    VALUES (%(courier_id)s, %(object_value)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value
                """,
                {
                    "courier_id": raw_courier.courier_id,
                    "object_value": raw_courier.object_value,
                },
            )

    def insert_deliveries(self, conn, raw_delivery: RawCourier) -> None:
        with conn.cursor() as cur:
            print("Insert deliveries data")
            cur.execute(
                """
                    INSERT INTO stg.deliveries(delivery_id, object_value)
                    VALUES (%(delivery_id)s, %(object_value)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value
                """,
                {
                    "delivery_id": raw_delivery.delivery_id,
                    "object_value": raw_delivery.object_value,
                },
            )

    def load_couriers(self):
        with self.stg.connection() as conn:
            print("Load connection")
            wf_setting = self.settings_repository.get_setting(
                conn, self.wf_key_couriers
            )
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key_couriers,
                    workflow_settings={"last_loaded_offset": 0},
                )
            last_loaded_offset = wf_setting.workflow_settings["last_loaded_offset"]
            print("Last loaded offset {}".format(last_loaded_offset))
            while True:
                load_queue = self.get_couriers(last_loaded_offset)
                if len(load_queue) == 0:
                    print("Have not data")
                    break
                else:
                    print("Iterate for courier")
                    # iterate for courier
                    for load_data in load_queue:
                        raw_courier = RawCourier(
                            courier_id=load_data["_id"],
                            object_value=json.dumps(load_data),
                        )
                        print(
                            "Raw data id {} obj value {}".format(
                                raw_courier.courier_id, raw_courier.object_value
                            )
                        )
                        # insert into database
                        self.insert_couriers(conn, raw_courier)
                    last_loaded_offset += 50
                    continue

            wf_setting.workflow_settings["last_loaded_offset"] = last_loaded_offset
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )
            print(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)

    def load_deliveries(self):
        with self.stg.connection() as conn:
            print("Load connection")
            wf_setting = self.settings_repository.get_setting(
                conn, self.wf_key_deliveries
            )
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key_deliveries,
                    workflow_settings={"last_loaded_offset": 0},
                )
            last_loaded_offset = wf_setting.workflow_settings["last_loaded_offset"]
            print("Last loaded offset {}".format(last_loaded_offset))
            while True:
                load_queue = self.get_deliveries(last_loaded_offset)
                if len(load_queue) == 0:
                    print("Have not data")
                    break
                else:
                    print("Iterate for deliveries")
                    # iterate for delivery
                    for load_data in load_queue:
                        raw_delivery = RawDelivery(
                            delivery_id=load_data["delivery_id"],
                            object_value=json.dumps(load_data),
                        )
                        print(
                            "Raw data id {} obj value {}".format(
                                raw_delivery.delivery_id, raw_delivery.object_value
                            )
                        )
                        # insert into database
                        self.insert_deliveries(conn, raw_delivery)
                    last_loaded_offset += 50
                    continue

            wf_setting.workflow_settings["last_loaded_offset"] = last_loaded_offset
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )
            print(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)


@dag(
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2023, 5, 12, tz="UTC"),
    catchup=False,
    tags=["origin2stg"],
    is_paused_upon_creation=True,
)
def stg_deliveries_dag():
    dwh_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="stg_courier_load")
    def load():
        delivery_loader = RawAPIDataLoader(dwh_connect)
        delivery_loader.load_deliveries()

    load_dict = load()
    load_dict  #


stg_couriers_dag = stg_deliveries_dag()
