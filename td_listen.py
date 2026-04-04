#!/usr/bin/env python3
"""
Listen to the Network Rail Combined TD feed and print activity at berth 1757.

TD message types:
  CA - Berth step (train moves from one berth to another)
  CB - Berth cancel (train removed from berth)
  CC - Berth interpose (train appears in a berth, e.g. at start of shift)
  SF - Signalling update (individual signal/point state)
  SG, SH - Signalling refresh

We care about CA messages where 'to' berth is 1757.
"""

import json
import os
import signal
import sys
from datetime import datetime

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

WATCH_BERTH = "1757"


def make_consumer() -> Consumer:
    return Consumer({
        "bootstrap.servers": os.environ["TD_KAFKA_BOOTSTRAP"],
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": os.environ["TD_KAFKA_USERNAME"],
        "sasl.password": os.environ["TD_KAFKA_PASSWORD"],
        "group.id": os.environ["TD_KAFKA_GROUP"],
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })


def handle_message(raw: str):
    try:
        messages = json.loads(raw)
    except json.JSONDecodeError:
        return

    if not isinstance(messages, list):
        messages = [messages]

    for msg in messages:
        for msg_type, body in msg.items():
            if msg_type == "CA_MSG":
                to_berth = body.get("to", "")
                from_berth = body.get("from", "")
                headcode = body.get("descr", "").strip()
                area = body.get("area_id", "")
                ts = body.get("time", "")

                if WATCH_BERTH in (to_berth, from_berth):
                    direction = "INTO" if to_berth == WATCH_BERTH else "OUT OF"
                    now = datetime.now().strftime("%H:%M:%S")
                    print(f"[{now}] {direction} berth {WATCH_BERTH} — "
                          f"headcode={headcode!r}  area={area}  "
                          f"from={from_berth} → to={to_berth}  (td_ts={ts})")


def main():
    for var in ("TD_KAFKA_BOOTSTRAP", "TD_KAFKA_USERNAME",
                "TD_KAFKA_PASSWORD", "TD_KAFKA_GROUP", "TD_KAFKA_TOPIC"):
        if not os.environ.get(var):
            print(f"Error: {var} not set in .env")
            sys.exit(1)

    topic = os.environ["TD_KAFKA_TOPIC"]
    consumer = make_consumer()
    consumer.subscribe([topic])

    def shutdown(sig, frame):
        print("\nShutting down...")
        consumer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)

    print(f"Listening on topic '{topic}', watching berth {WATCH_BERTH}...")
    print("Press Ctrl+C to stop.\n")

    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                print(f"Kafka error: {msg.error()}")
            continue
        handle_message(msg.value().decode("utf-8", errors="replace"))


if __name__ == "__main__":
    main()
