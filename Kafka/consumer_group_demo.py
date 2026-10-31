"""
Kafka Consumer Group Demo - Shows horizontal scaling
Run multiple instances of this script to see partition assignment

Usage:
    # Terminal 1
    python consumer_group_demo.py consumer-1

    # Terminal 2 (while above is running)
    python consumer_group_demo.py consumer-2

    # Terminal 3 (while above are running)
    python consumer_group_demo.py consumer-3

    Watch the output: each terminal should get different partitions!
"""

from kafka import KafkaConsumer
import json
from datetime import datetime
import sys
import time

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'user-events'
GROUP_ID = 'scaling-demo-group'

def create_consumer(consumer_name):
    """Create a consumer with debugging output"""
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        session_timeout_ms=10000  # Quick rebalancing for demo
    )
    return consumer, consumer_name

def main():
    """Main consumer loop with rebalance detection"""
    consumer_name = sys.argv[1] if len(sys.argv) > 1 else 'consumer-1'

    consumer, name = create_consumer(consumer_name)

    print(f"\n{'='*60}")
    print(f"Consumer: {name}")
    print(f"Topic: {TOPIC}")
    print(f"Consumer Group: {GROUP_ID}")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVERS}")
    print(f"{'='*60}\n")

    # Show initial partition assignment
    partitions = consumer.assignment()
    print(f"✓ Initial partition assignment: {partitions}")
    print(f"Waiting for messages...\n")

    message_count = 0
    start_time = datetime.now()
    last_partitions = partitions

    try:
        for message in consumer:
            # Check if rebalancing happened
            current_partitions = consumer.assignment()
            if current_partitions != last_partitions:
                print(f"\n⚡ REBALANCE DETECTED!")
                print(f"  Old assignment: {last_partitions}")
                print(f"  New assignment: {current_partitions}\n")
                last_partitions = current_partitions

            event = message.value
            message_count += 1

            # Display message with partition info
            print(f"[{name}] Message from Partition {message.partition}, "
                  f"Offset {message.offset}: "
                  f"{event['user_id']} → {event['event']}")

            # Summary every 20 messages
            if message_count % 20 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = message_count / elapsed if elapsed > 0 else 0
                print(f"\n📊 {name} Summary: {message_count} messages, "
                      f"{rate:.1f} msgs/sec, "
                      f"Assigned partitions: {current_partitions}\n")

    except KeyboardInterrupt:
        print(f"\n\n{name} shutting down...")
    finally:
        consumer.close()
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\n{name} stopped.")
        print(f"Total messages: {message_count}")
        print(f"Duration: {elapsed:.2f}s")

if __name__ == '__main__':
    main()
