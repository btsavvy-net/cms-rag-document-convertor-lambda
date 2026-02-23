"""
dynamodb_service.py
-------------------

Purpose:
This file updates an existing document item in the DynamoDB `rag-docs` table
after PDF → IR conversion is successfully completed.

Key points:
- Uses UpdateItem (partial update)
- Does NOT overwrite the full DynamoDB item
- Preserves all classification attributes written earlier
  (mime, mime_class, flags, content_sha256, etc.)

When used:
- Called by the PDF Converter Lambda
- After IR JSONL is uploaded to S3
- Before sending the document to the chunking stage
"""

import boto3
from datetime import datetime, timezone

from core.settings import settings
from core.logging import setup_logger

logger = setup_logger(__name__)

# Cached DynamoDB resource (Lambda execution reuse)
_dynamodb = None


# -------------------------------------------------
# DynamoDB Resource (Lazy Initialization)
# -------------------------------------------------
def get_dynamodb_resource():
    """
    Returns a reusable DynamoDB resource.

    Why this exists:
    - Lambda execution environments can be reused
    - Reusing AWS clients improves performance
    - Avoids unnecessary client creation on every invocation
    """
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource(
            "dynamodb",
            region_name=settings.AWS_REGION,
        )
    return _dynamodb


def get_docs_table():
    """
    Returns the DynamoDB Table object for `rag-docs`.

    Table name comes from environment configuration via settings.
    """
    return get_dynamodb_resource().Table(settings.DOCS_TABLE)


# -------------------------------------------------
# Public API: Update document conversion status
# -------------------------------------------------
def update_document_status(
    tenant_id: str,
    doc_id: str,
    ir_s3_key: str,
    page_count: int,
    element_count: int,
    table_count: int,
    pymupdf_version: str = "1.24.8",
    camelot_version: str = "0.11.0",
):
    """
    Updates PDF conversion details for a document item in DynamoDB.

    DynamoDB table schema (ACTUAL):
    - Partition key : tenant_id
    - Sort key      : doc_id  (value format: DOC#{doc_id})

    Attributes updated:
    - status        → "converted"
    - conversion    → Map (PDF conversion metadata)
    - updated_at    → ISO8601 UTC timestamp

    IMPORTANT:
    - Uses UpdateItem with SET
    - All existing classification fields remain untouched
    """

    logger.info(
        "Updating DynamoDB document conversion status | "
        f"tenant_id={tenant_id}, doc_id={doc_id}"
    )

    table = get_docs_table()
    now_time = datetime.now(timezone.utc).isoformat()

    # -------------------------------------------------
    # Partial update: only required fields are modified
    # -------------------------------------------------
    table.update_item(
        Key={
            # MUST match actual DynamoDB key names
            "tenant_id": tenant_id,
            "doc_id": f"DOC#{doc_id}",
        },
        UpdateExpression="""
            SET
                #status = :status,
                conversion = :conversion,
                updated_at = :updated_at
        """,
        ExpressionAttributeNames={
            # 'status' is a DynamoDB reserved word
            "#status": "status",
        },
        ExpressionAttributeValues={
            ":status": "converted",
            ":updated_at": now_time,
            ":conversion": {
                "stage": "pdf",
                "ir_s3_key": ir_s3_key,
                "page_count": page_count,
                "element_count": element_count,
                "table_count": table_count,
                "extractor": {
                    "pymupdf": pymupdf_version,
                    "camelot": camelot_version,
                },
                "timings_ms": {},  # Required by spec (can be populated later)
                "schema_version": settings.IR_SCHEMA_VERSION,
            },
        },
    )

    logger.info("DynamoDB document updated successfully (status=converted)")
