from decouple import config as env


KAFKA_BROKER = env("KAFKA_BROKER", default="localhost:9092")
KAFKA_TOPIC = env("KAFKA_TOPIC", default="creditcard")

MINIO_ENDPOINT = env("MINIO_ENDPOINT", default="http://minio:9000")
MINIO_ACCESS_KEY = env("MINIO_ACCESS_KEY", default="minioadmin")
MINIO_SECRET_KEY = env("MINIO_SECRET_KEY", default="minioadmin")
MINIO_BUCKET = env("MINIO_BUCKET", default="silver")

WINDOW_SIZE_SECONDS = 24
