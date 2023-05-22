from typing import List

from connector import PgConnect
from psycopg.rows import class_row
from pydantic import BaseModel


class PreReport(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    rate_total_sum: int
    orders_total_sum: int
    courier_tips_sum: int
    orders_count: int


class Report(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: int
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: int
    courier_reward_sum: float


class DataLoader:
    def __init__(self, db: PgConnect) -> None:
        self.db = db
        self.batch = 1
        self.api_nickname = "NICKNAME"
        self.api_cohort = "12"
        self.api_key = "API_KEY"
        self.api_header = {
            "X-Nickname": self.api_nickname,
            "X-Cohort": self.api_cohort,
            "X-API-KEY": self.api_key,
        }

    def get_report(self, conn: PgConnect) -> List[PreReport]:
        with conn.cursor(row_factory=class_row(PreReport)) as cur:
            cur.execute(
                """
                   WITH cte AS (
                   SELECT dc.courier_id AS courier_id, dc.courier_name AS courier_name, dt.year AS settlement_year, 
                   dt.month AS settlement_month, dd.rate::float, dd.sum::integer, dd.tip_sum::integer, dd.order_id, dd.delivery_id
                   FROM dds.dm_delivery dd 
                   INNER JOIN dds.dm_couriers dc ON dd.courier_id = dc.id
                   INNER JOIN dds.dm_orders do2 ON dd.order_id=do2.id
                   INNER JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id),
                   precounted AS (SELECT courier_id, courier_name, settlement_year, settlement_month, 
                   SUM(rate) AS rate_total_sum, SUM(sum) AS orders_total_sum, SUM(tip_sum) as courier_tips_sum, 
                   COUNT(order_id) as orders_count 
                   FROM cte 
                   GROUP BY courier_id, settlement_year, settlement_month, courier_name)
                   SELECT * FROM precounted;
                """,
            )
            obj = cur.fetchall()
        return obj

    @staticmethod
    def get_courier_order_sum(orders_total_sum: int, courier_rate: float):
        if courier_rate < 4:
            courier_order_sum = orders_total_sum * 0.05
            if courier_order_sum < 100:
                return 100
            return courier_order_sum
        if 4 <= courier_rate < 4.5:
            courier_order_sum = orders_total_sum * 0.07
            if courier_order_sum < 150:
                return 150
            return courier_order_sum
        if 4.5 <= courier_rate < 4.9:
            courier_order_sum = orders_total_sum * 0.08
            if courier_order_sum < 175:
                return 175
            return courier_order_sum
        if courier_rate >= 4.9:
            courier_order_sum = orders_total_sum * 0.1
            if courier_order_sum < 200:
                return 200
            return courier_order_sum

    @staticmethod
    def get_courier_reward_sum(courier_order_sum, courier_tips_sum):
        return courier_order_sum + (courier_tips_sum * 0.95)

    @staticmethod
    def get_avg_rate(total_rate, orders_count):
        return total_rate / orders_count

    def insert_report(self, conn, report: Report) -> None:
        with conn.cursor() as cur:
            print("Insert report data")
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(courier_id, courier_name, settlement_year, settlement_month, 
                    orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, 
                    courier_tips_sum, courier_reward_sum)
                    VALUES (%(courier_id)s, %(courier_name)s, %(settlement_year)s, %(settlement_month)s,
                     %(orders_count)s, %(orders_total_sum)s, %(rate_avg)s, %(order_processing_fee)s, 
                     %(courier_order_sum)s, %(courier_tips_sum)s, %(courier_reward_sum)s)
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET
                        orders_count = EXCLUDED.orders_count,
                        courier_name = EXCLUDED.courier_name,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum
                """,
                {
                    "courier_id": report.courier_id,
                    "courier_name": report.courier_name,
                    "settlement_year": report.settlement_year,
                    "settlement_month": report.settlement_month,
                    "orders_count": report.orders_count,
                    "orders_total_sum": report.orders_total_sum,
                    "rate_avg": report.rate_avg,
                    "order_processing_fee": report.order_processing_fee,
                    "courier_order_sum": report.courier_order_sum,
                    "courier_tips_sum": report.courier_tips_sum,
                    "courier_reward_sum": report.courier_reward_sum,
                },
            )

    def load_report(self):
        with self.db.connection() as conn:
            print("Load connection")
            load_queue = self.get_report(conn)
            if len(load_queue) == 0:
                print("Have not data")
            else:
                # iterate for report
                for load_data in load_queue:
                    # insert into database
                    print("*** {}".format(load_data))
                    avr_rate = self.get_avg_rate(
                        load_data.rate_total_sum, load_data.orders_count
                    )
                    courier_order_sum = self.get_courier_order_sum(
                        load_data.orders_total_sum, avr_rate
                    )
                    courier_reward_sum = self.get_courier_reward_sum(
                        load_data.orders_total_sum, load_data.courier_tips_sum
                    )
                    report = Report(
                        courier_id=load_data.courier_id,
                        courier_name=load_data.courier_name,
                        settlement_year=load_data.settlement_year,
                        settlement_month=load_data.settlement_month,
                        orders_count=load_data.orders_count,
                        orders_total_sum=load_data.orders_total_sum,
                        rate_avg=avr_rate,
                        order_processing_fee=load_data.orders_total_sum * 0.25,
                        courier_order_sum=courier_order_sum,
                        courier_tips_sum=load_data.courier_tips_sum,
                        courier_reward_sum=courier_reward_sum,
                    )

                    self.insert_report(conn, report)
            return len(load_queue)
