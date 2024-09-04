import json
import logging
import requests
import topic_check

logger = logging.getLogger(__name__)

KSQL_URL = "http://localhost:8088"

KSQL_STATEMENT = """
CREATE TABLE turnstile (
    station_id INT,
    station_name VARCHAR,
    line VARCHAR
) WITH (
    KAFKA_TOPIC='raw_turnstile',
    VALUE_FORMAT='AVRO',
    KEY='station_id'
);

CREATE TABLE turnstile_summary
WITH (VALUE_FORMAT='JSON') AS
    SELECT station_id, COUNT(*) AS count
    FROM turnstile
    GROUP BY station_id;
"""

def execute_statement():
    """Executes the KSQL statement against the KSQL API"""
    if topic_check.topic_exists("TURNSTILE_SUMMARY"):
        logger.info("KSQL statement already executed, skipping.")
        return

    logger.debug("Executing KSQL statement...")

    resp = requests.post(
        f"{KSQL_URL}/ksql",
        headers={"Content-Type": "application/vnd.ksql.v1+json"},
        data=json.dumps(
            {
                "ksql": KSQL_STATEMENT,
                "streamsProperties": {"ksql.streams.auto.offset.reset": "earliest"},
            }
        ),
    )

    # Ensure that a 2XX status code was returned
    try:
        resp.raise_for_status()
        logger.info("KSQL statement executed successfully.")
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error executing KSQL statement: {e}")
        raise

if __name__ == "__main__":
    execute_statement()
