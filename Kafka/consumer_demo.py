"""
Kafka Consumer Demo - Consume and Display User Activity Events
Run this script to read events from Kafka

Usage:
    python consumer_demo.py
    python consumer_demo.py --from-beginning
"""

from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime
import sys

# Kafka configuration
BOOTSTRAP_SERVERS = 'localhost:9092'
TOPIC = 'user-events'
GROUP_ID = 'dashboard-consumer'

def create_consumer(from_beginning=False):
    """Create and return a Kafka consumer"""
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=[BOOTSTRAP_SERVERS],
        group_id=GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest' if from_beginning else 'latest',
        enable_auto_commit=True,
        auto_commit_interval_ms=1000
    )

def main():
    """Main consumer loop"""
    from_beginning = '--from-beginning' in sys.argv

    consumer = create_consumer(from_beginning)

    print(f"Kafka Consumer Started")
    print(f"Topic: {TOPIC}")
    print(f"Consumer Group: {GROUP_ID}")
    print(f"Bootstrap Server: {BOOTSTRAP_SERVERS}")
    print(f"Reading from: {'beginning' if from_beginning else 'latest messages'}")
    print(f"Partitions assigned: {consumer.assignment()}\n")
    print("Waiting for messages...\n")

    event_counts = defaultdict(int)
    user_counts = defaultdict(int)
    message_count = 0
    start_time = datetime.now()

    try:
        for message in consumer:
            event = message.value
            event_counts[event['event']] += 1
            user_counts[event['user_id']] += 1
            message_count += 1

            # Display individual message
            print(f"[Message {message_count}] "
                  f"Partition={message.partition}, "
                  f"Offset={message.offset}, "
                  f"Key={message.key.decode() if message.key else 'None'}")
            print(f"  User: {event['user_id']}")
            print(f"  Event: {event['event']}")
            print(f"  Page: {event['page']}")
            print(f"  Time: {event['timestamp']}")
            print()

            # Print summary every 10 messages
            if message_count % 10 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = message_count / elapsed if elapsed > 0 else 0

                print("=" * 60)
                print(f"DASHBOARD UPDATE - {message_count} messages processed")
                print(f"Rate: {rate:.2f} messages/sec")
                print(f"\nEvent Breakdown:")
                for event_type, count in sorted(event_counts.items()):
                    pct = (count / message_count) * 100
                    print(f"  {event_type}: {count} ({pct:.1f}%)")
                print(f"\nTop 3 Active Users:")
                for user, count in sorted(user_counts.items(),
                                        key=lambda x: x[1], reverse=True)[:3]:
                    print(f"  {user}: {count} events")
                print("=" * 60 + "\n")

    except KeyboardInterrupt:
        print("\n\nShutting down consumer...")
    finally:
        consumer.close()
        elapsed = (datetime.now() - start_time).total_seconds()
        print(f"\nConsumer stopped.")
        print(f"Total messages consumed: {message_count}")
        print(f"Duration: {elapsed:.2f}s")
        if elapsed > 0:
            print(f"Average rate: {message_count / elapsed:.2f} messages/sec")

if __name__ == '__main__':
    main()
