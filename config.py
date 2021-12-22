import os


class Config:
    KAFKA_SERVER = "localhost:9092"
    AWS_SERVER = os.environ["VM_SERVER"]
    TOPIC_ID = "test"
    GROUP_ID = "my-group"
    CLIENT_ID = "client-1"
    SESSION_TIMEOUT_MS = 1000
    OFFSET_REST = "earllest"

    # Consumer
    SETTINGS = {
        "bootstrap.servers": MY_SERVER,
        "group.id": GROUP_ID,
        "client.id": CLIENT_ID,
        "enable.auto.commit": True,
        "session.timeout.ms": SESSION_TIMEOUT_MS,
        "default.topic.config": {"auto.offset.reset": OFFSET_REST},
    }
