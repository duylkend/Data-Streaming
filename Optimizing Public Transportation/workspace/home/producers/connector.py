import json
import logging
import requests

logger = logging.getLogger(__name__)

KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created, skipping recreation")
        return

    # Kafka Connect Config to connect to Postgres and load the 'stations' table
    config = {
        "name": CONNECTOR_NAME,
        "config": {
            "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "batch.max.rows": "500",
            "connection.url": "jdbc:postgresql://localhost:5432/cta",
            "connection.user": "cta_admin",
            "connection.password": "chicago",
            "table.whitelist": "stations",
            "mode": "incrementing",
            "incrementing.column.name": "stop_id",
            "topic.prefix": "org.chicago.cta.",
            "poll.interval.ms": "60000",  # Poll every 60 seconds
        }
    }

    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={"Content-Type": "application/json"},
        data=json.dumps(config),
    )

    # Ensure a healthy response was given
    try:
        resp.raise_for_status()
        logging.debug("connector created successfully")
    except requests.exceptions.HTTPError as e:
        logging.error(f"Failed to create connector: {e}")

if __name__ == "__main__":
    configure_connector()
