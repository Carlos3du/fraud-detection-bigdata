import traceback
from io import BytesIO

import boto3
import pandas as pd


class MinioStorage:
    def __init__(
        self,
        endpoint: str,
        access_key: str,
        secret_key: str,
        bucket: str,
    ) -> None:
        self.bucket = bucket
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

    def ensure_bucket_exists(self) -> None:
        try:
            print("Checking existing MinIO buckets...")
            buckets = self.client.list_buckets().get("Buckets", [])
            bucket_names = [bucket["Name"] for bucket in buckets]
            print(f"Buckets found: {bucket_names}")
            if self.bucket not in bucket_names:
                print(f"Creating bucket: {self.bucket}")
                self.client.create_bucket(Bucket=self.bucket)
                print(f"Bucket created: {self.bucket}")
            else:
                print(f"Bucket already exists: {self.bucket}")
        except Exception as exc:
            print(f"Failed to initialize MinIO bucket '{self.bucket}': {exc}")
            traceback.print_exc()
            raise

    def upload_parquet(self, df: pd.DataFrame, window: int) -> str:
        buffer = BytesIO()
        object_key = f"transformed/window_24s={window}.parquet"

        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        print(f"Uploading parquet to MinIO: bucket={self.bucket}, key={object_key}")
        self.client.put_object(
            Bucket=self.bucket,
            Key=object_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )
        print(f"Upload completed: bucket={self.bucket}, key={object_key}")
        return object_key
