import time
from datetime import datetime, timedelta
import json
from uuid import uuid4
import random
import confluent_kafka as ck
from confluent_kafka import SerializingProducer


# DEFINE CONSTANTS
LAGOS_COORDINATES = {"latitude": 6.465422, "longitude": 3.406448}
ILORIN_COORDINATES = {"latitude": 8.500000, "longitude": 4.550000}

LATITUDE_INCREMENT = (ILORIN_COORDINATES["latitude"] - LAGOS_COORDINATES["latitude"]) / 1000
LONGITUDE_INCREMENT = (ILORIN_COORDINATES["longitude"] - LAGOS_COORDINATES["longitude"]) / 1000

VEHICLE_COORDINATES = LAGOS_COORDINATES.copy()

START_TIME = datetime.now()
TIME_INCREMENT = timedelta(seconds=10)

VEHICLE_HEALTH_STATUS = [
    "HEALTHY",
    "FAULTY",
    "SOMEWHAT HEALTHY"
]
WEATHER_STATUS = [
    "RAINY",
    "CLOUDY",
    "SUNNY",
]
TEMPERATURE_UPPER_BOUND = 45
TEMPERATURE_LOWER_BOUND = 29

# DEFINE TOPIC
VEHICLE_TOPIC = "vehicle_data"
LOCATION_TOPIC = "location_data"
WEATHER_TOPIC = "weather_data"

# DEFINE FUNCTIONS THAT GENERATES DATA FOR EACH TOPIC
def generate_vehicle_data(vehicle_name: str, time: datetime):
    data = {
        "id": str(uuid4()),
        "vehicle_name": vehicle_name,
        "timestamp": time.isoformat(),
        "health_status": random.choice(VEHICLE_HEALTH_STATUS)
    }
    return data

def generate_location_data(vehicle_name: str, time: datetime):
    data = {
        "id": str(uuid4()),
        "vehicle_name": vehicle_name,
        "timestamp": time.isoformat(),
        "location": VEHICLE_COORDINATES
    }
    return data

def generate_weather_data(vehicle_name: str, time: datetime):
    data = {
        "id": str(uuid4()),
        "vehicle_name": vehicle_name,
        "timestamp": time.isoformat(),
        "weather_status": random.choice(WEATHER_STATUS),
        "temperature": random.uniform(TEMPERATURE_LOWER_BOUND, TEMPERATURE_UPPER_BOUND)
    }
    return data

def produce_to_kafka(producer: SerializingProducer, topic: str, data: dict):
    producer.produce(
        topic=topic,
        key=data["id"],
        value=json.dumps(data),
    )


# DEFINE A FUNCTION THAT SIMULATES THE JOURNEY
def simulate_journey():
    CURRENT_TIME = START_TIME

    # DEFINE KAFKA PRODUCER
    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "error_cb": lambda err: print(f"kafka error: {err}")
    }
    producer = SerializingProducer(producer_config)

    while VEHICLE_COORDINATES["latitude"] <= (ILORIN_COORDINATES["latitude"]) \
        and VEHICLE_COORDINATES["longitude"] <= (ILORIN_COORDINATES["longitude"]):
        vehicle_data = generate_vehicle_data("MAYBACH-G4", CURRENT_TIME)
        location_data = generate_location_data("MAYBACH-G4", CURRENT_TIME)
        weather_data = generate_weather_data("MAYBACH-G4", CURRENT_TIME)

        # print(f"""
        #     vehicle_data    :   {int(vehicle_data['time'].timestamp())}\n
        #     location_data   :   {location_data}\n
        #     weather_data    :   {weather_data}\n
        #     ===============================================================================
        # """)

        # PRODUCE DATA TO KAFKA
        produce_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_to_kafka(producer, LOCATION_TOPIC, location_data)
        produce_to_kafka(producer, WEATHER_TOPIC, weather_data)

        # UPDATE COORDINATES AND TIME
        VEHICLE_COORDINATES["latitude"] += LATITUDE_INCREMENT
        VEHICLE_COORDINATES["longitude"] += LONGITUDE_INCREMENT
        CURRENT_TIME += TIME_INCREMENT
        time.sleep(0.1)
    producer.flush()


if __name__ == "__main__":
    simulate_journey()
