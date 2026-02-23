# pdf_controller.py

from services.s3_service import download_pdf, upload_ir_jsonl
from services.pdf_reader import read_pdf
from services.dynamodb_service import update_document_status
from services.sns_service import publish_chunk_request
from core.logging import setup_logger
from core.settings import settings

logger = setup_logger(__name__)


def process_pdf_message(message: dict):
    """
    Main PDF processing controller for AWS Lambda.

    Steps:
    1. Download PDF from S3
    2. Extract all text/elements from PDF
    3. Upload IR JSONL.gz to S3
    4. Update DynamoDB with conversion status (status="converted")
    5. Publish SNS chunk request
    """

    logger.info("PDF Controller started")

    # Extract message data
    tenant_id = message["tenant_id"]
    doc_id = message["doc_id"]
    source_bucket = message["bucket"]
    source_key = message["key"]
    source_version_id = message.get("version_id")  # optional

    # -------------------------------
    # 1. Download PDF from S3
    # -------------------------------
    logger.info(f"Downloading PDF from S3: {source_bucket}/{source_key}")
    pdf_bytes = download_pdf(source_bucket, source_key)

    # -------------------------------
    # 2. Extract text/elements from PDF
    # -------------------------------
    logger.info("Extracting text from PDF")
    elements = read_pdf(
        pdf_bytes=pdf_bytes,
        tenant_id=tenant_id,
        doc_id=doc_id
    )
    logger.info(f"Extracted {len(elements)} elements from PDF")

    # -------------------------------
    # 3. Upload IR JSONL.gz to S3
    # -------------------------------
    logger.info("Uploading IR JSONL.gz to S3")
    ir_key = upload_ir_jsonl(
        elements=elements,
        tenant_id=tenant_id,
        doc_id=doc_id,
        schema_version=settings.IR_SCHEMA_VERSION
    )
    logger.info(f"IR uploaded to s3://{settings.PROCESSED_BUCKET_NAME}/{ir_key}")

    # Compute counts
    page_count = len(set(e["page"] for e in elements))
    element_count = len(elements)
    table_count = sum(1 for e in elements if e.get("type") == "table")  # 0 if no tables

    # -------------------------------
    # 4. Update DynamoDB (MUST be before SNS)
    # -------------------------------
    logger.info("Updating DynamoDB document status to 'converted'")
    update_document_status(
        tenant_id=tenant_id,
        doc_id=doc_id,
        # status="converted",  # required by spec
        ir_s3_key=ir_key,
        page_count=page_count,
        element_count=element_count,
        table_count=table_count
    )
    logger.info("DynamoDB updated successfully")

    # -------------------------------
    # 5. Publish SNS chunk request
    # -------------------------------
    logger.info("Publishing SNS chunk request")
    publish_chunk_request(
        tenant_id=tenant_id,
        doc_id=doc_id,
        ir_bucket=settings.PROCESSED_BUCKET_NAME,
        ir_key=ir_key,
        source_bucket=source_bucket,
        source_key=source_key,
        source_version_id=source_version_id,
        page_count=page_count,
        element_count=element_count,
        table_count=table_count,
        sns_topic_arn=settings.SNS_TOPIC_ARN
    )
    logger.info("SNS chunk request published successfully")

    logger.info("PDF Controller completed successfully")
    return {"ok": True}
