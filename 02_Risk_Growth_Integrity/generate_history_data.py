# generate_history_data.py
import os
import time
import uuid
import random
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker

fake = Faker('zh_CN')
np.random.seed(42)
random.seed(42)

TOTAL_RECORDS = 1_000_000   # 历史数据量，本地建议 100 万行
BATCH_SIZE = 100_000
OUTPUT_DIR = "data/raw_events.parquet"

os.makedirs("data", exist_ok=True)

# 1. 预设黑产实体池 (用于生成具有网络拓扑和统计聚集特征的数据)
FRAUD_IPS = [f"192.168.10.{i}" for i in range(1, 10)] + [f"10.200.5.{i}" for i in range(1, 5)]
FRAUD_DEVICES = [f"device_farm_hack_{i:04d}" for i in range(20)]
BASE_ADDRESSES = [
    "北京市朝阳区酒仙桥路4号院751大厦",
    "广东省深圳市南山区高新南九道科技园",
    "上海市浦东新区张江高科碧波路690号"
]
ADDR_VARIANTS = ["东门代收", "门卫转交", "自提点旁边", "菜鸟驿站后", "A座大堂", "南门保安室"]

# 裂变链生成 (单线超深裂变)
FRAUD_CHAINS = [[f"fraud_user_c{c}_{i:03d}" for i in range(20)] for c in range(10)]

def generate_record(event_time_ms):
    is_fraud = np.random.rand() < 0.08  # 8% 黑产流量
    event_id = str(uuid.uuid4())
    
    if not is_fraud:
        # 正常业务流量
        user_id = f"user_{random.randint(100000, 999999)}"
        device_id = f"dev_{random.randint(100000, 999999)}"
        ip = fake.ipv4()
        event_type = random.choices(
            ['RECEIVE_COUPON', 'CREATE_ORDER', 'PAY_ORDER', 'REFUND_ORDER', 'INVITE_REGISTER'],
            weights=[0.3, 0.35, 0.25, 0.05, 0.05]
        )[0]
        coupon_id = f"coup_{random.randint(1, 50)}" if event_type == 'RECEIVE_COUPON' else None
        order_id = f"ord_{uuid.uuid4().hex[:12]}" if event_type in ['CREATE_ORDER', 'PAY_ORDER', 'REFUND_ORDER'] else None
        amount = round(random.uniform(10.0, 500.0), 2) if order_id else None
        address = fake.address() if order_id else None
        referrer_id = f"user_{random.randint(100000, 999999)}" if event_type == 'INVITE_REGISTER' else None
        is_refund = 1 if event_type == 'REFUND_ORDER' else 0
        fraud_label = 0
    else:
        # 黑产异常注入
        fraud_type = random.choice(["FARMING", "FAST_REFUND", "DEEP_CHAIN", "ADDR_FUZZ"])
        fraud_label = 1
        
        if fraud_type == "FARMING":
            event_type = 'RECEIVE_COUPON'
            user_id = f"farm_bot_{random.randint(1, 5000)}"
            device_id = random.choice(FRAUD_DEVICES)
            ip = random.choice(FRAUD_IPS)
            coupon_id = "coup_super_free_100"
            order_id, amount, address, referrer_id, is_refund = None, None, None, None, 0

        elif fraud_type == "FAST_REFUND":
            event_type = random.choice(['CREATE_ORDER', 'REFUND_ORDER'])
            user_id = f"scalper_{random.randint(1, 200)}"
            device_id = f"scalper_dev_{random.randint(1, 20)}"
            ip = random.choice(FRAUD_IPS)
            order_id = f"fraud_ord_{random.randint(1, 1000)}"
            amount = round(random.uniform(2000.0, 8000.0), 2)
            address = random.choice(BASE_ADDRESSES)
            coupon_id, referrer_id = None, None
            is_refund = 1 if event_type == 'REFUND_ORDER' else 0

        elif fraud_type == "DEEP_CHAIN":
            event_type = 'INVITE_REGISTER'
            selected_chain = random.choice(FRAUD_CHAINS)
            node_idx = random.randint(1, len(selected_chain) - 1)
            user_id = selected_chain[node_idx]
            referrer_id = selected_chain[node_idx - 1]
            device_id = f"chain_dev_{node_idx}"
            ip = random.choice(FRAUD_IPS)
            coupon_id, order_id, amount, address, is_refund = None, None, None, None, 0

        elif fraud_type == "ADDR_FUZZ":
            event_type = 'CREATE_ORDER'
            user_id = f"addr_bot_{random.randint(1, 500)}"
            device_id = random.choice(FRAUD_DEVICES)
            ip = fake.ipv4()
            order_id = f"fuzz_ord_{uuid.uuid4().hex[:10]}"
            amount = 99.0
            coupon_id = "coup_super_free_100"
            address = f"{random.choice(BASE_ADDRESSES)}{random.choice(ADDR_VARIANTS)}"
            referrer_id, is_refund = None, 0

    return {
        "event_id": event_id,
        "event_time": event_time_ms,
        "event_type": event_type,
        "user_id": user_id,
        "device_id": device_id,
        "ip": ip,
        "coupon_id": coupon_id,
        "order_id": order_id,
        "amount": amount,
        "address": address,
        "referrer_id": referrer_id,
        "is_refund": is_refund,
        "is_fraud_label": fraud_label
    }

def main():
    print(f"[*] 开始生成近 30 天离线历史数据，目标总行数: {TOTAL_RECORDS}...")
    now = datetime.now()
    start_time = now - timedelta(days=30)
    start_ts = int(start_time.timestamp() * 1000)
    end_ts = int(now.timestamp() * 1000)

    # 对应设计的统一 Schema
    schema = pa.schema([
        ('event_id', pa.string()),
        ('event_time', pa.int64()),
        ('event_type', pa.string()),
        ('user_id', pa.string()),
        ('device_id', pa.string()),
        ('ip', pa.string()),
        ('coupon_id', pa.string()),
        ('order_id', pa.string()),
        ('amount', pa.float64()),
        ('address', pa.string()),
        ('referrer_id', pa.string()),
        ('is_refund', pa.int32()),
        ('is_fraud_label', pa.int32()),
    ])

    records = []
    writer = None
    batch_count = 0
    
    for i in range(TOTAL_RECORDS):
        curr_ts = random.randint(start_ts, end_ts)
        records.append(generate_record(curr_ts))
        
        if len(records) >= BATCH_SIZE:
            df_batch = pd.DataFrame(records)
            df_batch.sort_values(by="event_time", inplace=True)
            table = pa.Table.from_pandas(df_batch, schema=schema)
            
            if writer is None:
                writer = pq.ParquetWriter(OUTPUT_DIR, schema)
            writer.write_table(table)
            records = []
            batch_count += 1
            print(f"    -> 已写入批次 {batch_count}/{(TOTAL_RECORDS // BATCH_SIZE)}")

    if records:
        df_batch = pd.DataFrame(records)
        df_batch.sort_values(by="event_time", inplace=True)
        table = pa.Table.from_pandas(df_batch, schema=schema)
        if writer is None:
            writer = pq.ParquetWriter(OUTPUT_DIR, schema)
        writer.write_table(table)

    if writer:
        writer.close()
    print(f"[✓] 离线数据生成完成，输出路径: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()