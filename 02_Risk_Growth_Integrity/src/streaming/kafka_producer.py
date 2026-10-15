# kafka_producer.py
import json
import time
import uuid
import random
import argparse
from concurrent.futures import ThreadPoolExecutor
from confluent_kafka import Producer
from faker import Faker

fake = Faker('zh_CN')

TOPIC_NAME = "realtime_risk_events"
BOOTSTRAP_SERVERS = "localhost:9092"

PRODUCER_CONFIG = {
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'queue.buffering.max.messages': 500000,
    'queue.buffering.max.kbytes': 1024000,
    'batch.num.messages': 2000,
    'linger.ms': 10,
    'compression.type': 'lz4',
    'acks': 1
}

producer = Producer(PRODUCER_CONFIG)

def delivery_report(err, msg):
    pass

def get_realtime_event():
    now_ms = int(time.time() * 1000)
    is_fraud = random.random() < 0.12  # 实时流量中黑产占比
    
    if not is_fraud:
        user_id = f"user_{random.randint(100000, 999999)}"
        device_id = f"dev_{random.randint(100000, 999999)}"
        event_type = random.choices(['RECEIVE_COUPON', 'CREATE_ORDER', 'PAY_ORDER'], weights=[0.4, 0.4, 0.2])[0]
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": now_ms,
            "event_type": event_type,
            "user_id": user_id,
            "device_id": device_id,
            "ip": fake.ipv4(),
            "coupon_id": "coup_normal_5" if event_type == 'RECEIVE_COUPON' else None,
            "order_id": f"ord_{uuid.uuid4().hex[:10]}" if event_type != 'RECEIVE_COUPON' else None,
            "amount": round(random.uniform(20.0, 300.0), 2) if event_type != 'RECEIVE_COUPON' else None,
            "address": fake.address() if event_type != 'RECEIVE_COUPON' else None,
            "referrer_id": None,
            "is_refund": 0
        }
    else:
        fraud_mode = random.choice(["SPAM_COUPON", "FAST_REFUND"])
        if fraud_mode == "SPAM_COUPON":
            event = {
                "event_id": str(uuid.uuid4()),
                "event_time": now_ms,
                "event_type": "RECEIVE_COUPON",
                "user_id": f"farm_bot_{random.randint(1, 1000)}",
                "device_id": "device_farm_hack_0001",
                "ip": "192.168.10.1",
                "coupon_id": "coup_super_free_100",
                "order_id": None,
                "amount": 100.0,
                "address": None,
                "referrer_id": None,
                "is_refund": 0
            }
        else:
            event = {
                "event_id": str(uuid.uuid4()),
                "event_time": now_ms,
                "event_type": "REFUND_ORDER",
                "user_id": f"scalper_{random.randint(1, 50)}",
                "device_id": "scalper_dev_01",
                "ip": "192.168.10.2",
                "coupon_id": None,
                "order_id": f"fraud_ord_{random.randint(1, 100)}",
                "amount": 5000.0,
                "address": "北京市朝阳区酒仙桥路4号院751大厦A座大堂",
                "referrer_id": None,
                "is_refund": 1
            }
    return event

def produce_worker(target_count):
    sent = 0
    for _ in range(target_count):
        payload = json.dumps(get_realtime_event()).encode('utf-8')
        try:
            producer.produce(TOPIC_NAME, value=payload, on_delivery=delivery_report)
            sent += 1
            if sent % 1000 == 0:
                producer.poll(0)
        except BufferError:
            producer.poll(0.1)
            producer.produce(TOPIC_NAME, value=payload, on_delivery=delivery_report)
    producer.flush()

def main():
    parser = argparse.ArgumentParser(description="高吞吐 Kafka 风险事件压测生产器")
    parser.add_argument("--qps", type=int, default=3000, help="每秒推送消息数")
    parser.add_argument("--duration", type=int, default=30, help="持续时间(秒)")
    args = parser.parse_args()

    print(f"[*] 启动 Kafka 实时生产，Topic: {TOPIC_NAME}，速率: {args.qps} QPS...")
    
    start_time = time.time()
    total_messages = 0

    with ThreadPoolExecutor(max_workers=4) as executor:
        for second in range(args.duration):
            sec_start = time.time()
            chunk = args.qps // 4
            futures = [executor.submit(produce_worker, chunk) for _ in range(4)]
            for f in futures:
                f.result()
            
            total_messages += args.qps
            cost = time.time() - sec_start
            sleep_time = max(0.0, 1.0 - cost)
            time.sleep(sleep_time)
            
            actual_rate = int(args.qps / (cost + sleep_time))
            print(f"  [第 {second+1:02d} 秒] 已推送 {total_messages} 条数据 | 瞬时吞吐: {actual_rate} msg/s")

    producer.flush()
    elapsed = time.time() - start_time
    print(f"[✓] 推送完毕！累计发送: {total_messages} 条，耗时: {elapsed:.2f}s，平均 QPS: {int(total_messages / elapsed)}")

if __name__ == "__main__":
    main()