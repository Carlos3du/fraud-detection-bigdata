import atexit
import json

import pandas as pd
from kafka import KafkaConsumer

from config import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    MINIO_ACCESS_KEY,
    MINIO_BUCKET,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    WINDOW_SIZE_SECONDS,
)
from storage import MinioStorage
from transformer import TransactionTransformer


class SilverLayerConsumer:
    def __init__(
        self,
        kafka_consumer: KafkaConsumer,
        storage: MinioStorage,
        transformer: TransactionTransformer,
    ) -> None:
        self.kafka_consumer = kafka_consumer
        self.storage = storage
        self.transformer = transformer
        self.current_window: int | None = None
        self.current_window_rows: list[pd.DataFrame] = []

    def flush_current_window(self) -> str | None:
        if self.current_window is None or not self.current_window_rows:
            return None

        window_df = pd.concat(self.current_window_rows, ignore_index=True)
        object_key = self.storage.upload_parquet(window_df, self.current_window)
        self.current_window_rows = []
        return object_key

    def process_message(self, message) -> None:
        print(
            f"Received Kafka message: partition={message.partition}, "
            f"offset={message.offset}"
        )

        transformed_df = self.transformer.transform_record(message.value)
        if transformed_df is None:
            print(
                "Skipping Kafka message after validation: "
                f"partition={message.partition}, offset={message.offset}"
            )
            return

        record_window = int(
            transformed_df.iloc[0]["transaction_time"] // WINDOW_SIZE_SECONDS
        )

        if self.current_window is None:
            self.current_window = record_window

        if record_window != self.current_window:
            object_key = self.flush_current_window()
            if object_key is not None:
                print(f"24-second parquet flushed: {object_key}")
            self.current_window = record_window

        self.current_window_rows.append(transformed_df)

        print(
            json.dumps(
                {
                    "input_topic": KAFKA_TOPIC,
                    "minio_bucket": MINIO_BUCKET,
                    "current_window": self.current_window,
                    "buffered_rows": len(self.current_window_rows),
                    "record": transformed_df.iloc[0].to_dict(),
                },
                ensure_ascii=False,
            )
        )

    def run(self) -> None:
        for message in self.kafka_consumer:
            self.process_message(message)


def create_kafka_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )


def create_storage() -> MinioStorage:
    storage = MinioStorage(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        bucket=MINIO_BUCKET,
    )
    storage.ensure_bucket_exists()
    return storage


def main() -> None:
    app = SilverLayerConsumer(
        kafka_consumer=create_kafka_consumer(),
        storage=create_storage(),
        transformer=TransactionTransformer(),
    )

    @atexit.register
    def flush_on_shutdown() -> None:
        object_key = app.flush_current_window()
        if object_key is not None:
            print(f"Final 24-second parquet flushed on shutdown: {object_key}")

    app.run()


if __name__ == "__main__":
    main()
