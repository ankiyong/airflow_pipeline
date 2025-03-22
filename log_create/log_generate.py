import csv
import time
import random
import psycopg2
from psycopg2.extras import execute_values

csv_file = '/app/olist.csv'
DB_PARAMS = {
    "host": "192.168.56.40",
    "port": 30001,
    "database": "postgres",
    "user": "postgres",
    "password": "595855"
}
FIELDNAMES = [
    "order_id", "customer_id", "order_status", "order_purchase_timestamp",
    "order_approved_at", "order_delivered_carrier_date", "order_delivered_customer_date",
    "order_estimated_delivery_date", "payment_type", "payment_sequential",
    "payment_installments", "payment_value", "order_item_id", "product_id",
    "seller_ids", "shipping_limit_date", "price", "freight_value"
]
def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS order_log (
                log_time TIMESTAMP,
                order_id VARCHAR(32),
                customer_id VARCHAR(32),
                order_status TEXT,
                order_purchase_timestamp TIMESTAMP,
                order_approved_at TIMESTAMP,
                order_delivered_carrier_date TIMESTAMP,
                order_delivered_customer_date TIMESTAMP,
                order_estimated_delivery_date TIMESTAMP,
                payment_type TEXT,
                payment_sequential INTEGER,
                payment_installments INTEGER,
                payment_value NUMERIC(10,2),
                order_item_id INTEGER,
                product_id TEXT,
                seller_ids TEXT[],
                shipping_limit_date TIMESTAMP,
                price NUMERIC(10,2),
                freight_value NUMERIC(10,2)
            );
        """
        )
        conn.commit()
def insert_log(conn,logs):
    with conn.cursor() as cur:
        sql = """
            INSERT INTO order_log (
                log_time, order_id, customer_id, order_status, order_purchase_timestamp,
                order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
                order_estimated_delivery_date, payment_type, payment_sequential, payment_installments,
                payment_value, order_item_id, product_id, seller_ids, shipping_limit_date, price, freight_value
            )
            VALUES %s
        """
        execute_values(cur, sql, logs)
        conn.commit()


def parse_array(array_str):
    if array_str.startswith('{') and array_str.endswith('}'):
        content = array_str[1:-1].strip()
        if content:
            return [item.strip() for item in content.split(',')]
    return []

def parse_timestamp(ts_str):
    return ts_str if ts_str.strip() != '' else None

def log_generate():
    conn = psycopg2.connect(**DB_PARAMS)
    create_table(conn)
    logs = []
    batch_size = 100
    with open(csv_file, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(reader, None)
        for row in reader:
            if not row:
                continue
            log_time = time.strftime('%Y-%m-%d %H:%M:%S')
            order_id = row[0]
            customer_id = row[1]
            order_status = row[2]
            order_purchase_timestamp = parse_timestamp(row[3])
            order_approved_at = parse_timestamp(row[4])
            order_delivered_carrier_date = parse_timestamp(row[5])
            order_delivered_customer_date = parse_timestamp(row[6])
            order_estimated_delivery_date = parse_timestamp(row[7])
            payment_type = row[8]
            payment_sequential = int(row[9])
            payment_installments = int(row[10])
            payment_value = float(row[11])
            order_item_id = int(row[12])
            product_id = row[13]
            seller_ids = parse_array(row[14])
            shipping_limit_date = parse_timestamp(row[15])
            price = float(row[16])
            freight_value = float(row[17])
            logs.append((
                log_time, order_id, customer_id, order_status, order_purchase_timestamp,
                order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
                order_estimated_delivery_date, payment_type, payment_sequential, payment_installments,
                payment_value, order_item_id, product_id, seller_ids, shipping_limit_date, price, freight_value
            ))

            if len(logs) >= batch_size:
                insert_log(conn, logs)
                logs = []
                time.sleep(2)
    if logs:
        insert_log(conn, logs)
        time.sleep(2)
    conn.close()

if __name__ == '__main__':
    log_generate()

