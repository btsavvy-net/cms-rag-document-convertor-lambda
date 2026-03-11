import boto3
import json
import gzip
from io import BytesIO

from core.settings import settings
from core.logging import setup_logger

logger = setup_logger(__name__)


def get_s3_client():
    """
    Create and return an S3 client using the configured AWS region.
    Defaults to 'us-east-2' if not set in settings.
    """
    return boto3.client(
        "s3",
        region_name=getattr(settings, "AWS_REGION", "us-east-2")
    )


def download_pdf(bucket_name: str, object_key: str) -> bytes:
    """
    Download a PDF file from S3.

    Args:
        bucket_name (str): S3 bucket name
        object_key (str): S3 object key (path)

    Returns:
        bytes: PDF file content
    """
    logger.info(f"Downloading PDF from S3: {bucket_name}/{object_key}")
    s3_client = get_s3_client()

    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    pdf_bytes = response["Body"].read()

    logger.info("PDF download completed")
    return pdf_bytes


def upload_ir_jsonl(
    elements: list,
    tenant_id: str,
    doc_id: str,
    schema_version: str = "ir_v1"
) -> str:
    """
    Upload extracted PDF elements as compressed JSONL to S3.

    Args:
        elements (list): List of extracted elements from PDF
        tenant_id (str): Tenant identifier
        doc_id (str): Document identifier
        schema_version (str): Schema version for IR (default: "ir_v1")

    Returns:
        str: S3 key of uploaded JSONL.gz file
    """
    s3_client = get_s3_client()
    bucket_name = settings.PROCESSED_BUCKET_NAME
    s3_key = f"{tenant_id}/{doc_id}/ir/{schema_version}/elements.jsonl.gz"

    logger.info(f"Uploading IR file to S3: {bucket_name}/{s3_key}")

    # Sort elements to maintain reading order
    # Safe handling when bbox is None (common in OCR outputs)
    elements_sorted = sorted(
        elements,
        key=lambda e: (
            e.get("page", 0),
            (e.get("bbox") or {}).get("y0", 0),
            (e.get("bbox") or {}).get("x0", 0),
        )
    )

    # Write JSONL.gz in memory
    buffer = BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb") as gz_file:
        for element in elements_sorted:
            line = json.dumps(element, ensure_ascii=False) + "\n"
            gz_file.write(line.encode("utf-8"))

    buffer.seek(0)

    # Upload to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/json",
        ContentEncoding="gzip",
        Tagging=f"tenant_id={tenant_id}&doc_id={doc_id}&schema={schema_version}"
    )

    logger.info("IR file upload successful")

    return s3_key


def update_source_object_tags(
    bucket_name: str,
    object_key: str,
    has_tables: bool = False
):
    """
    Update tags on the original PDF to indicate it has been processed.

    Args:
        bucket_name (str): S3 bucket name
        object_key (str): PDF object key
        has_tables (bool): Whether the PDF contains tables
    """
    logger.info("Updating source PDF tags")

    s3_client = get_s3_client()

    tags = {
        "processed_stage": "converted",
        "has_tables": str(has_tables).lower()
    }

    s3_client.put_object_tagging(
        Bucket=bucket_name,
        Key=object_key,
        Tagging={
            "TagSet": [
                {"Key": k, "Value": v}
                for k, v in tags.items()
            ]
        }
    )

    logger.info("Source PDF tags updated")
