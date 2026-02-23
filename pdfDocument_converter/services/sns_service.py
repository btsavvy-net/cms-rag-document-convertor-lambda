"""
sns_service.py

Responsibilities:
- Publish a message to SNS
- Trigger the next pipeline stage: rag chunking
"""

import json
import boto3
from datetime import datetime, timezone
import uuid

from core.settings import settings
from core.logging import setup_logger

logger = setup_logger(__name__)

_sns_client = None


# -------------------------------
# SNS client (lazy creation)
# -------------------------------
def get_sns_client():
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client(
            "sns",
            region_name=settings.AWS_REGION
        )
    return _sns_client


def publish_chunk_request(
    tenant_id: str,
    doc_id: str,
    ir_bucket: str,
    ir_key: str,
    source_bucket: str,
    source_key: str,
    source_version_id: str | None,
    page_count: int,
    element_count: int,
    table_count: int,
    sns_topic_arn: str | None = None,
):
    """
    Publish a chunking request to SNS.
    Contract MUST match chunker expectations.
    """

    sns_topic_arn = sns_topic_arn or settings.SNS_TOPIC_ARN

    if not sns_topic_arn:
        logger.warning("SNS_TOPIC_ARN not configured. Skipping SNS publish.")
        return None

    # -------------------------------
    # Build message body
    # -------------------------------
    message = {
        "tenant_id": tenant_id,
        "doc_id": doc_id,
        "ir_s3": {
            "bucket": ir_bucket,
            "key": ir_key,
        },
        "source_pdf": {
            "bucket": source_bucket,
            "key": source_key,
            "version_id": source_version_id,
        },
        "stats": {
            "page_count": page_count,
            "element_count": element_count,
            "table_count": table_count,
        },
        "schema_version": settings.IR_SCHEMA_VERSION,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "trace_id": f"trc-{uuid.uuid4().hex}",
    }

    logger.info(
        "Publishing chunk request to SNS "
        f"(tenant_id={tenant_id}, doc_id={doc_id})"
    )

    # -------------------------------
    # Publish to SNS with required attributes
    # -------------------------------
    response = get_sns_client().publish(
        TopicArn=sns_topic_arn,
        Message=json.dumps(message),
        MessageAttributes={
            "tenant_id": {"DataType": "String", "StringValue": tenant_id},
            "doc_id": {"DataType": "String", "StringValue": doc_id},
            "ir_version": {"DataType": "String", "StringValue": "v1"},
            "stage": {"DataType": "String", "StringValue": "ir_ready"},
        },
    )

    logger.info(
        f"SNS publish successful. MessageId={response.get('MessageId')}"
    )

    return response
