from kafka import KafkaConsumer
import json
import time

def process_event(event):
    # Simulate processing logic
    print(f"Processing event: {event}")
    if event['event_value'] < 0.1:  # Simulate a failure condition
        raise Exception("Simulated processing failure")

def main():
    consumer = KafkaConsumer(
        'events',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=False
    )

    for message in consumer:
        event = message.value
        try:
            process_event(event)
            consumer.commit()  # Commit the message offset only after successful processing
        except Exception as e:
            print(f"Error processing event {event}: {e}")
            # Implement retry logic here (e.g., retry a fixed number of times, log to a file, etc.)
            time.sleep(1)  # Simple retry mechanism

if __name__ == "__main__":
    main()

