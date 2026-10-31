"""
Kafka Producer Demo - Produce User Activity Events
Run this script to send sample events to Kafka

Usage:
    python producer_demo.py
"""

from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import time
from datetime import datetime
import random
import sys

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'user-events'

# Sample data
USERS = ['user_001', 'user_002', 'user_003', 'user_004', 'user_005']
EVENTS = ['page_view', 'click', 'purchase', 'logout', 'login']
PAGES = ['/home', '/products', '/cart', '/checkout', '/dashboard']

def create_producer():
    """Create and return a Kafka producer"""
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all',  # Wait for all replicas
        enable_idempotence=True,  # No duplicates on retry
        retries=3
    )

def generate_event():
    """Generate a random user event"""
    user = random.choice(USERS)
    event = random.choice(EVENTS)

    return {
        'user_id': user,
        'event': event,
        'page': random.choice(PAGES),
        'timestamp': datetime.utcnow().isoformat(),
        'session_id': f"session_{random.randint(1000, 9999)}",
        'duration_ms': random.randint(100, 5000)
    }

def on_send_success(record_metadata):
    """Callback for successful send"""
    print(f"✓ Message sent: topic={record_metadata.topic}, "
          f"partition={record_metadata.partition}, "
          f"offset={record_metadata.offset}")

def on_send_error(exc):
    """Callback for send error"""
    print(f"✗ Error sending message: {exc}")

def main():
    """Main producer loop"""
    producer = create_producer()

    print(f"Kafka Producer Started")
    print(f"Topic: {TOPIC}")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVERS}\n")

    event_count = 0

    try:
        while True:
            # Generate and send event
            event = generate_event()

            # Send with key (for partitioning) and callbacks
            future = producer.send(
                TOPIC,
                value=event,
                key=event['user_id'].encode('utf-8')
            )

            # Add callbacks
            future.add_callback(on_send_success)
            future.add_errback(on_send_error)

            event_count += 1
            print(f"[{event_count}] Producing: {event['user_id']} → {event['event']}")

            # Wait 1 second between messages (adjust for different throughput)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\nShutting down producer...")
    finally:
        producer.flush()  # Ensure all messages sent
        producer.close()
        print(f"Producer stopped. Total events sent: {event_count}")

if __name__ == '__main__':
    main()
