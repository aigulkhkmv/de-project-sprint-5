import os.path
import sys
from datetime import datetime
from typing import Dict, List

from connector import PgConnect
from psycopg.rows import class_row
from pydantic import BaseModel

sys.path.append(os.path.realpath('../'))
from scripts import json2str
from stg2dds.srv_table_update import StgEtlSettingsRepository


class EtlSetting(BaseModel):
    id: int
    workflow_key: str
    workflow_settings: Dict


class Courier(BaseModel):
    id: int
    courier_id: str
    courier_name: str


class Delivery(BaseModel):
    id: int
    order_id: int
    delivery_id: str
    courier_id: int
    address: str
    delivery_ts: datetime
    rate: str
    sum: str
    tip_sum: str


class StgDataLoader:
    def __init__(self, stg: PgConnect) -> None:
        self.stg = stg
        self.settings_repository = StgEtlSettingsRepository()
        self.wf_key_couriers = "couriers_stg_to_dds_workflow"
        self.wf_key_deliveries = "deliveries_stg_to_dds_workflow"
        self.batch = 1
        self.api_nickname = "aigulkhkmv"
        self.api_cohort = "12"
        self.api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
        self.api_header = {
            "X-Nickname": self.api_nickname,
            "X-Cohort": self.api_cohort,
            "X-API-KEY": self.api_key,
        }

    def get_deliveries(self, conn: PgConnect, threshold: int, limit: int) -> List[Delivery]:
        with conn.cursor(row_factory=class_row(Delivery)) as cur:
            cur.execute(
                """
                   with cte as (SELECT id, object_value::JSON->>'delivery_id' as delivery_id, 
                   object_value::JSON->>'order_id' as order_id, object_value::JSON->>'courier_id' as courier_id, 
                   object_value::JSON->>'address' as address, object_value::JSON->>'delivery_ts' as delivery_ts, 
                   object_value::JSON->>'rate' as rate, object_value::JSON->>'sum' as sum, 
                   object_value::JSON->>'tip_sum' as tip_sum FROM stg.deliveries
                   )
                   SELECT cte.id, cte.delivery_id, do2.id as order_id, dc.id as courier_id, cte.address, cte.delivery_ts, cte.rate, cte.sum, cte.tip_sum FROM cte 
                   INNER JOIN dds.dm_couriers dc ON dc.courier_id = cte.courier_id 
                   INNER JOIN dds.dm_orders do2 ON cte.order_id = do2.order_key 
                   WHERE cte.id > %(threshold)s ORDER BY cte.id ASC LIMIT %(limit)s;
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            obj = cur.fetchall()
        return obj

    def get_couriers(self, conn: PgConnect, threshold: int, limit: int) -> List[Courier]:
        with conn.cursor(row_factory=class_row(Courier)) as cur:
            cur.execute(
                """
                   SELECT id, object_value::json->>'_id' AS courier_id, object_value::json->>'name' AS courier_name 
                   FROM stg.couriers WHERE id > %(threshold)s ORDER BY id ASC LIMIT %(limit)s;
                """,
                {
                    "threshold": threshold,
                    "limit": limit
                }
            )
            obj = cur.fetchall()
        return obj

    def insert_couriers(self, conn: PgConnect, courier: Courier) -> None:
        with conn.cursor() as cur:
            print("Insert couriers data")
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name,
                },
            )

    def insert_deliveries(self, conn, delivery: Delivery) -> None:
        with conn.cursor() as cur:
            print("Insert deliveries data")
            print("DELIVERY {}".format(delivery.order_id))
            cur.execute(
                """
                    INSERT INTO dds.dm_delivery(delivery_id, order_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(delivery_id)s, %(order_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "order_id": delivery.order_id,
                    "courier_id": delivery.courier_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts,
                    "rate": delivery.rate,
                    "sum": delivery.sum,
                    "tip_sum": delivery.tip_sum
                },
            )

    def load_couriers(self):
        with self.stg.connection() as conn:
            print("Load connection")
            wf_setting = self.settings_repository.get_setting(conn, self.wf_key_couriers)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.wf_key_couriers, workflow_settings={"last_loaded_offset": -1})
            last_loaded_offset = wf_setting.workflow_settings["last_loaded_offset"]
            print("Last loaded offset {}".format(last_loaded_offset))
            load_queue = self.get_couriers(conn, threshold=last_loaded_offset, limit=self.batch)
            if len(load_queue) == 0:
                print("Have not data")
            else:
                print("Iterate for courier")
                # iterate for courier
                for load_data in load_queue:
                    # insert into database
                    courier = Courier(id=load_data.id, courier_id=load_data.courier_id, courier_name=load_data.courier_name)
                    self.insert_couriers(conn, courier)

            wf_setting.workflow_settings["last_loaded_offset"] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )
            print(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)

    def load_deliveries(self):
        with self.stg.connection() as conn:
            print("Load connection")
            wf_setting = self.settings_repository.get_setting(conn, self.wf_key_deliveries)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.wf_key_deliveries, workflow_settings={"last_loaded_offset": -1})
            last_loaded_offset = wf_setting.workflow_settings["last_loaded_offset"]
            print("Last loaded offset {}".format(last_loaded_offset))
            load_queue = self.get_deliveries(conn, threshold=last_loaded_offset, limit=self.batch)
            if len(load_queue) == 0:
                print("Have not data")
            else:
                print("Iterate for delivery")
                # iterate for courier
                for load_data in load_queue:
                    # insert into database
                    delivery = Delivery(id=load_data.id, order_id=load_data.order_id, delivery_id=load_data.delivery_id,
                                        courier_id=load_data.courier_id, address=load_data.address,
                                        delivery_ts=load_data.delivery_ts, rate=load_data.rate,
                                        sum=load_data.sum, tip_sum=load_data.tip_sum)
                    self.insert_deliveries(conn, delivery)

            wf_setting.workflow_settings["last_loaded_offset"] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )
            print(f"Finishing work. Last checkpoint: {wf_setting_json}")
            return len(load_queue)
