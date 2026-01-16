# produce_send_rows.py
import os
import time
import csv
import json
import argparse
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError

def send_payload(producer: EventHubProducerClient, payload: str):
    """
    Create a batch, add payload (handle batch overflow), and send.
    """
    try:
        batch = producer.create_batch()
    except EventHubError as e:
        raise RuntimeError(f"Failed to create batch: {e}")

    event = EventData(payload)


    try:
        batch.add(event)
        producer.send_batch(batch)
        return True
    except ValueError:


        try:
            single = producer.create_batch(max_size_in_bytes=1024*1024*4)  # attempt larger (4MB)
            single.add(event)
            producer.send_batch(single)
            return True
        except Exception as e2:
            print(f"[ERROR] Single event too large to send or other error: {e2}")
            return False
    except Exception as e:
        print(f"[ERROR] Failed sending batch: {e}")
        return False

def csv_row_generator(csv_path):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            yield r

def main():
    ap = argparse.ArgumentParser(description="Send CSV rows to Azure Event Hub (one row per interval)")
    ap.add_argument("--csv", required=True, help="Path to CSV file")
    ap.add_argument("--eh_conn", default=None, help="Event Hub namespace connection string (or use env EVENTHUB_CONN)")
    ap.add_argument("--eh_name", required=True, help="Event Hub (entity) name")
    ap.add_argument("--interval", type=int, default=900, help="Seconds between sends (default 900=15min)")
    ap.add_argument("--loop", action="store_true", help="Loop the CSV repeatedly")
    args = ap.parse_args()

    conn_str = args.eh_conn or os.environ.get("EVENTHUB_CONN")
    if not conn_str:
        raise SystemExit("Missing Event Hub connection string: pass --eh_conn or set EVENTHUB_CONN env var")

    producer = EventHubProducerClient.from_connection_string(conn_str, eventhub_name=args.eh_name)

    gen = csv_row_generator(args.csv)
    try:
        while True:
            try:
                row = next(gen)
            except StopIteration:
                if args.loop:
                    gen = csv_row_generator(args.csv)
                    row = next(gen)
                else:
                    print("All rows sent; exiting.")
                    break

            # add a produced timestamp
            row["produced_at"] = int(time.time())
            payload = json.dumps(row, ensure_ascii=False)

            ok = send_payload(producer, payload)
            if ok:
                print(f"Sent row -> EventHub='{args.eh_name}' ts={row['produced_at']}")
            else:
                print("Failed to send row; continuing.")

            time.sleep(args.interval)
    finally:
        try:
            producer.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
