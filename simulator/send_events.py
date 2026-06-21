import random
import time
from datetime import datetime, timezone

import requests


API_URL = "http://localhost:8000/events"


def make_event(client_id: str, entity_id: str) -> dict:
    return {
        "client_id": client_id,
        "event_type": "order_created",
        "entity_id": entity_id,
        "event_time": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "amount": random.randint(100, 5000),
            "city": random.choice(["Hyderabad", "Bengaluru", "Mumbai", "Delhi"]),
        },
    }


def send_event(event: dict):
    response = requests.post(API_URL, json=event, timeout=5)
    print(response.status_code, response.json())


def normal_traffic():
    for i in range(20):
        event = make_event("seller_normal", f"ORD-N-{i}")
        send_event(event)
        time.sleep(0.2)


def replay_traffic():
    duplicate_event = make_event("seller_replay", "ORD-DUP-1")

    for _ in range(20):
        send_event(duplicate_event)
        time.sleep(0.2)


if __name__ == "__main__":
    print("Sending normal traffic...")
    normal_traffic()

    print("Sending replay traffic...")
    replay_traffic()