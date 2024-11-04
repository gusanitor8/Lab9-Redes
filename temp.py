import numpy as np
import random
import json
import time
from kafka import KafkaProducer


KAFKA_SERVER = 'lab9.alumchat.lol:9092'
TOPIC = '21438' 


def get_temperature(mean_temp: int, std_dev: int):
    res = np.random.normal(mean_temp, std_dev, 1)[0]
    return round(res, 2)

def get_humidity(mean_humidity: int, std_dev: int):
    humidity_sample = np.random.normal(mean_humidity, std_dev, 1)
    res = np.clip(humidity_sample, 0, 100)[0]
    return round(res, 2)

def get_wind_dir():
    directions = ["N", "NO", "O", "SO", "S", "SE", "E", "NE"]
    return random.choice(directions)

def to_dict(temp: int, humidity: int, wind_dir: str):
    dictionary = {
        "temperatura": temp,
        "humedad": humidity,
        "direccion_viento": wind_dir
    }

    return json.dumps(dictionary)

def send_data(producer):
    while True:
        temperature = get_temperature(mean_temp=15, std_dev=5)
        humidity = get_humidity(mean_humidity=60, std_dev=15)    
        wind_dir = get_wind_dir()
        data = to_dict(temperature, humidity, wind_dir)

        # Genera y envía los datos        
        producer.send(TOPIC, data)
        print(f"Enviado: {data}")

        # Espera entre 15 y 30 segundos antes de enviar el siguiente dato
        time_to_sleep = random.randint(15, 30)
        time.sleep(time_to_sleep)

def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    try:
        send_data(producer)
    finally:
        # Ensure all messages are sent
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()