from kafka import KafkaProducer
import json
import time
import random

def create_event():
    return {
        "event_id": random.randint(1, 1000),
        "event_timestamp": time.time(),
        "event_value": random.random()
    }

def main():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        event = create_event()
        producer.send('events', event)
        print(f"Produced event: {event}")
        time.sleep(1)  # Produce an event every second

if __name__ == "__main__":
    main()

