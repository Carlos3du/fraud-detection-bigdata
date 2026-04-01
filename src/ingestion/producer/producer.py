import sys

from kafka import KafkaProducer
import json
import pandas as pd
from decouple import config as env
from pathlib import Path
import subprocess

KAFKA_BROKER = env('KAFKA_BROKER', default='localhost:9092')
KAFKA_TOPIC = env('KAFKA_TOPIC', default='creditcard')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

script_path = Path("src/scripts/download_csv.py")
data_path = Path("dados/creditcard.csv")

if not data_path.exists():
    print("Baixando dataset...")
    subprocess.run([sys.executable, str(script_path)], check=True)
    
df = pd.read_csv(data_path)
for _, row in df.iterrows():
    producer.send(KAFKA_TOPIC, row.to_dict())

producer.flush()