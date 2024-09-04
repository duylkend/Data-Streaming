import logging
import time
from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # Configure the broker properties
        self.broker_properties = {
            "bootstrap.servers": "PLAINTEXT://localhost:9092",
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.broker_properties["bootstrap.servers"]
            },
            schema_registry=CachedSchemaRegistryClient(
                {
                    "url": self.broker_properties["schema.registry.url"]
                }
            ),
            default_key_schema=self.key_schema,
            default_value_schema=self.value_schema
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient({"bootstrap.servers": self.broker_properties["bootstrap.servers"]})
        topic = NewTopic(
            self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas
        )

        existing_topics = client.list_topics().topics
        if self.topic_name not in existing_topics:
            client.create_topics([topic])
            logger.info(f"Topic {self.topic_name} created")
        else:
            logger.info(f"Topic {self.topic_name} already exists")

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer:
            self.producer.flush()
            logger.info("Producer flushed and closed")

    def time_millis(self):
        """Returns current time in milliseconds"""
        return int(round(time.time() * 1000))
