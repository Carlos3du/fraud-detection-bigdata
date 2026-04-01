from kafka import KafkaConsumer
import json
import pandas as pd
from decouple import config as env

KAFKA_BROKER = env('KAFKA_BROKER', default='localhost:9092')
KAFKA_TOPIC = env('KAFKA_TOPIC', default='creditcard')

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)


for message in consumer:
    df = pd.DataFrame([message.value])
    
    # TODO: transformação e envio aqui
    print(df)