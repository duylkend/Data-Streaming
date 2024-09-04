import logging
import json

logger = logging.getLogger(__name__)

class Weather:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the weather model"""
        self.temperature = 70.0
        self.status = "sunny"

    def process_message(self, message):
        """Handles incoming weather data"""
        try:
            # Parse the message assuming it is in JSON format
            weather_json = json.loads(message.value())

            # Extract temperature and status from the message
            self.temperature = weather_json.get("temperature", self.temperature)
            self.status = weather_json.get("status", self.status)

            logger.info("Processed weather data - Temperature: %s, Status: %s", self.temperature, self.status)

        except KeyError as e:
            logger.error("Missing key in weather message: %s", e)
        except json.JSONDecodeError as e:
            logger.error("Invalid JSON format: %s", e)
        except Exception as e:
            logger.error("Error processing weather message: %s", e)
