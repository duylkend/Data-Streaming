"""Creates a turnstile data producer"""
import logging
from pathlib import Path

from confluent_kafka import avro
from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware

logger = logging.getLogger(__name__)

class Turnstile(Producer):
    # Load schemas only once when the class is first accessed
    key_schema = avro.load(Path(__file__).parent / "schemas/turnstile_key.json")
    value_schema = avro.load(Path(__file__).parent / "schemas/turnstile_value.json")

    def __init__(self, station):
        """Creates the Turnstile Producer"""
        super().__init__(
            topic_name="raw_turnstile",
            key_schema=Turnstile.key_schema,
            value_schema=Turnstile.value_schema,
            num_partitions=3,
            num_replicas=3,
        )
            
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)
        
        if num_entries <= 0:
            return

        # Prepare the value to avoid constructing it repeatedly in the loop
        value = {
            "station_id": self.station.station_id,
            "station_name": self.station.name,
            "line": self.station.color.name
        }

        # Produce messages for each entry
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,
                key={"timestamp": self.time_millis()},
                value=value,
            )
