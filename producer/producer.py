import json
import time
import random
from datetime import datetime, timedelta, timezone
from confluent_kafka import Producer

# Konfigurasi Kafka Producer
producer_config = {
    "bootstrap.servers": "kafka:9092" # Sesuaikan dengan alamat broker Kafka Anda
}
producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"""
✅ Transaction Event Delivered
Topic      : {msg.topic()}
Partition  : {msg.partition()}
Offset     : {msg.offset()}
Key        : {msg.key().decode('utf-8')}
Value      : {msg.value().decode('utf-8')}
""")
        
def generate_event(scenario="normal"):
    now = datetime.now(timezone.utc)
    
    # Template event standar
    event = {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": random.randint(50000, 500000),
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": random.choice(["mobile", "web", "pos"])
    }

    # Modifikasi data berdasarkan skenario simulasi
    if scenario == "invalid_amount":
        event["amount"] = random.randint(-50000, -500)
    elif scenario == "invalid_timestamp":
        event["timestamp"] = random.choice([now.strftime("%Y/%m/%d"),now.strftime("%d-%m-%y")])
    elif scenario == "invalid_source":
        event["source"] = random.choice(["satellite","tv"])
    elif scenario == "late_event":
        late_time = now - timedelta(minutes=5)
        event["timestamp"] = late_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    return event

print("🚀 Transaction Producer is running...")

# List skenario untuk simulasi
scenarios = [
    "normal", "normal", "normal", "normal", 
    "invalid_amount", "invalid_timestamp", "invalid_source", 
    "late_event", "late_event", "late_event",
    "duplicate"
]

last_event = None

try:
    while True:
        scenario = random.choice(scenarios)
        
        if scenario == "duplicate" and last_event is not None:
            event = last_event
        else:
            event = generate_event(scenario)
            last_event = event

        if scenario != "normal":
            print(f"⚠️ Sending {scenario} Event...")
        value = json.dumps(event).encode("utf-8")
        
        producer.produce(
            topic="transactions",
            key=event.get("user_id", "unknown").encode("utf-8"),
            value=value,
            callback=delivery_report
        )
        
        producer.flush()
        
        # Jeda pengiriman setiap 1 s.d 2 detik
        time.sleep(random.uniform(1, 2))

except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")