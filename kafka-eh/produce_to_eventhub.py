#!/usr/bin/env python3
# produce_to_eventhub.py
"""
Stream a CSV to Azure Event Hubs (Kafka endpoint) using confluent-kafka Producer.

Usage example:
  export EVENTHUB_CONN='Endpoint=sb://eh-flight-delays.servicebus.windows.net/;SharedAccessKeyName=send-policy;SharedAccessKey=...;EntityPath=airline-delays'
  python produce_to_eventhub.py --csv ./data/Airline_Delay_Cause.csv --bootstrap eh-flight-delays.servicebus.windows.net:9093 --topic airline-delays --batch 1000
"""

import os
import argparse
import csv
import json
import time
from confluent_kafka import Producer, KafkaException

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--bootstrap", required=True, help="bootstrap.servers (Event Hub FQDN:9093)")
    p.add_argument("--topic", required=True, help="Target Event Hub (topic) / entity path")
    p.add_argument("--batch", type=int, default=1000, help="Number messages between flushes")
    p.add_argument("--key-field", default=None, help="CSV column to use as message key (optional)")
    p.add_argument("--delimiter", default=",", help="CSV delimiter (default ',')")
    return p.parse_args()

def make_producer(bootstrap):

    conf = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "$ConnectionString", 
        "sasl.password": os.environ.get("EVENTHUB_CONN"), 
        # tuning
        "queue.buffering.max.messages": 1000000,
        "queue.buffering.max.ms": 1000,
        "message.timeout.ms": 300000,
    }
    if not conf["sasl.password"]:
        raise RuntimeError("EVENTHUB_CONN environment variable is not set. Export your full Event Hubs connection string.")
    return Producer(conf)

def delivery_report(err, msg, stats):
    if err is not None:
        stats['failed'] += 1
        print(f"[DELIVERY FAILED] topic={msg.topic()} partition={msg.partition()} error={err}")
    else:
        stats['delivered'] += 1




def stream_csv(producer, csv_path, topic, batch_size, key_field=None, delimiter=","):
    total = 0
    stats = {"delivered": 0, "failed": 0}
    last_report = time.time()

    with open(csv_path, newline='', encoding='utf-8') as fh:
        reader = csv.DictReader(fh, delimiter=delimiter)
        if key_field and key_field not in reader.fieldnames:
            print(f"Warning: key-field '{key_field}' not in CSV columns. Ignoring key.")
            key_field = None


        print("Starting produce loop...")
        batch_counter = 0

        for row in reader:
            total += 1
            # Convert row to JSON string 
            value = json.dumps(row)
            key = None
            if key_field:
                key = str(row.get(key_field, ""))

            try:
                producer.produce(topic=topic, value=value.encode("utf-8"), key=(key.encode("utf-8") if key else None),
                                 callback=lambda err, msg, s=stats: delivery_report(err, msg, s))
            except BufferError:

                producer.poll(1)
                producer.flush(5)
                producer.produce(topic=topic, value=value.encode("utf-8"), key=(key.encode("utf-8") if key else None),
                                 callback=lambda err, msg, s=stats: delivery_report(err, msg, s))

            # service the delivery callbacks and let librdkafka do background work
            producer.poll(0)

            batch_counter += 1
            if batch_counter >= batch_size:
                # flush outstanding messages to give backpressure and reliability
                producer.flush(30)   # block up to 30s
                batch_counter = 0

            # occasional progress print
            if time.time() - last_report > 5:
                print(f"Produced ~{total} messages (delivered: {stats['delivered']}, failed: {stats['failed']})")
                last_report = time.time()

    # final flush and wait for delivery callbacks
    print("Finished enqueueing rows. Flushing remaining messages...")
    producer.flush(60)  # allow up to 60s to deliver outstanding messages

    print(f"Done. Total enqueued: {total}, delivered: {stats['delivered']}, failed: {stats['failed']}")
    return total, stats

def main():
    args = parse_args()
    print(f"CSV -> EventHub producer\nCSV: {args.csv}\nBootstrap: {args.bootstrap}\nTopic: {args.topic}\nBatch size: {args.batch}\n")
    producer = make_producer(args.bootstrap)
    try:
        total, stats = stream_csv(producer, args.csv, args.topic, args.batch, key_field=args.key_field, delimiter=args.delimiter)
    except KeyboardInterrupt:
        print("Interrupted by user. Flushing and exiting...")
        producer.flush(10)
    except Exception as e:
        print("Producer error:", e)
        raise

if __name__ == "__main__":
    main()
