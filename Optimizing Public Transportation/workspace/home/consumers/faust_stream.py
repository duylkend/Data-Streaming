import logging
import faust

logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool

# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

# Define the Faust app
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")

# Define the input Kafka Topic
input_topic = app.topic("org.chicago.cta.stations", value_type=Station)

# Define the output Kafka Topic
output_topic = app.topic("transformed_stations", partitions=1)

# Define a Faust Table
table = app.Table(
    "transformed_station_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=output_topic,
)

@app.agent(input_topic)
async def process_station(stations):
    async for station in stations:
        # Determine the line color
        line_color = ''
        if station.red:
            line_color = 'red'
        elif station.blue:
            line_color = 'blue'
        elif station.green:
            line_color = 'green'
        else:
            logger.debug(f"Can't parse line color for station_id = {station.station_id}")

        # Create a TransformedStation record
        transformed = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line_color
        )

        # Update the Faust Table
        table[station.station_id] = transformed

        # Log the transformation for debugging
        logger.info(f"Transformed station {station.station_id} to {transformed}")

        # Send the transformed record to the output topic
        await output_topic.send(value=transformed)

if __name__ == "__main__":
    app.main()
