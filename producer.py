import json
import os
import time
import random
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "water_quality")
INTERVAL = float(os.getenv("PRODUCER_INTERVAL_SEC", "1.0"))

SENSORS = [
    {"sensor_id": "S-001", "location": "DeLand_River"},
    {"sensor_id": "S-002", "location": "Lake_Beresford"},
    {"sensor_id": "S-003", "location": "Stetson_Pond"},
]

def clamp(x, lo, hi):
    return max(lo, min(hi, x))

def generate_event():
    s = random.choice(SENSORS)
    ph = clamp(random.gauss(7.2, 0.4), 5.5, 9.5)
    turb = clamp(random.gauss(3.5, 2.0), 0.0, 20.0)
    do = clamp(random.gauss(8.0, 1.5), 0.0, 14.0)
    temp = clamp(random.gauss(24.0, 3.0), 0.0, 40.0)
    cond = clamp(random.gauss(450, 120), 50, 1500)

    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "sensor_id": s["sensor_id"],
        "location": s["location"],
        "ph": round(ph, 3),
        "turbidity": round(turb, 3),
        "dissolved_oxygen": round(do, 3),
        "temperature_c": round(temp, 3),
        "conductivity": round(cond, 3),
    }

def connect_producer():
    delay = 1
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=50,
                acks="all",
                request_timeout_ms=20000,
            )
            # Force a metadata fetch to confirm broker is reachable
            p.bootstrap_connected()
            print(f"[producer] connected to Kafka at {BOOTSTRAP}")
            return p
        except NoBrokersAvailable:
            print(f"[producer] Kafka not ready at {BOOTSTRAP}. retrying in {delay}s...")
            time.sleep(delay)
            delay = min(delay * 2, 15)

def main():
    print(f"[producer] starting. topic={TOPIC}, bootstrap={BOOTSTRAP}")
    producer = connect_producer()

    while True:
        evt = generate_event()
        producer.send(TOPIC, evt)
        producer.flush()
        print("[producer] sent:", evt)
        time.sleep(INTERVAL)

if __name__ == "__main__":
    main()