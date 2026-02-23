# s3_client.py

"""
S3Client

Responsibilities:
- Low-level AWS S3 operations
- No business logic (PDFs, JSONL, IR, etc.)
- Acts as the "tool" for interacting with S3
"""

import boto3
from botocore.exceptions import ClientError


class S3Client:
    def __init__(self):
        """
        Initialize the S3 client using boto3.
        In Lambda, this uses the IAM role assigned to the function.
        """
        self.s3 = boto3.client("s3")

    def download_file_bytes(self, bucket: str, key: str) -> bytes:
        """
        Download a file from S3 and return its bytes.

        Parameters:
        - bucket: str → S3 bucket name
        - key: str → S3 object key (file path in bucket)

        Returns:
        - bytes → file content
        """
        response = self.s3.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def object_exists(self, bucket: str, key: str) -> bool:
        """
        Check if an object exists in S3.

        Parameters:
        - bucket: str → S3 bucket name
        - key: str → S3 object key

        Returns:
        - bool → True if exists, False otherwise
        """
        try:
            self.s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def upload_bytes(
        self,
        bucket: str,
        key: str,
        data: bytes,
        content_type: str,
        content_encoding: str,
        tags: dict,
    ):
        """
        Upload bytes to S3.

        Parameters:
        - bucket: str → S3 bucket name
        - key: str → S3 object key
        - data: bytes → file content
        - content_type: str → MIME type, e.g., application/json
        - content_encoding: str → e.g., gzip
        - tags: dict → S3 object tags, e.g., {"tenant_id": "123"}

        Example of tags dict: {"tenant_id": "123", "doc_id": "456"}
        """
        # Convert dict to AWS tag string format
        tag_string = "&".join([f"{k}={v}" for k, v in tags.items()])

        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            ContentEncoding=content_encoding,
            Tagging=tag_string,
        )

    def tag_object(self, bucket: str, key: str, tags: dict):
        """
        Add or update tags on an existing S3 object.

        Parameters:
        - bucket: str → S3 bucket name
        - key: str → S3 object key
        - tags: dict → key-value pairs for tags

        Example:
            {"processed_stage": "converted", "has_tables": "false"}
        """
        # Convert dict to list of tag objects for AWS
        tag_set = [{"Key": k, "Value": v} for k, v in tags.items()]

        self.s3.put_object_tagging(
            Bucket=bucket,
            Key=key,
            Tagging={"TagSet": tag_set},
        )
