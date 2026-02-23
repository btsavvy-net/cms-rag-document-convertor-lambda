"""
handler.py

AWS Lambda entry point for PDF Converter.
Triggered by SQS messages (which may wrap SNS messages).

Flow:
1. Receive SQS event with potentially multiple messages.
2. Detect whether the message is direct JSON or SNS-wrapped.
3. Extract the actual PDF message payload.
4. Hand over message to controller for PDF processing.
5. Errors are raised to trigger SQS retry / DLQ.
"""

import json
from core.logging import setup_logger
from controllers.pdf_controller import process_pdf_message

# ------------------------
# Logger
# ------------------------
logger = setup_logger(__name__)


def handler(event, context):
    """
    Lambda entry point.

    Parameters:
    - event: dict → AWS Lambda event payload (from SQS)
    - context: LambdaContext → runtime info (not used here)

    Returns:
    - dict → status message
    """

    logger.info("PDF Converter Lambda started")

    # SQS can batch multiple messages in one Lambda call
    records = event.get("Records", [])

    for record in records:
        try:
            logger.info("Received new SQS message")
            logger.info(f"SQS Body: {record.get('body')}")

            # ------------------------
            # Step 1: Extract SQS body
            # ------------------------
            sqs_body = record.get("body", "{}")
            sqs_payload = json.loads(sqs_body)

            # ------------------------
            # Step 2: Detect SNS wrapper
            # ------------------------
            # If the message comes via SNS, the actual payload is in 'Message'
            if "Message" in sqs_payload:
                try:
                    pdf_message = json.loads(sqs_payload["Message"])
                    logger.info("Detected SNS-wrapped message")
                except json.JSONDecodeError:
                    logger.warning("SNS 'Message' field is not valid JSON, using raw string")
                    pdf_message = sqs_payload["Message"]
            else:
                pdf_message = sqs_payload  # direct SQS JSON message

            # ------------------------
            # Step 3: Log PDF message info
            # ------------------------
            logger.info(
                f"Processing document: tenant_id={pdf_message.get('tenant_id')}, "
                f"doc_id={pdf_message.get('doc_id')}"
            )

            # ------------------------
            # Step 4: Process PDF
            # ------------------------
            process_pdf_message(pdf_message)

            logger.info("Message processed successfully")

        except Exception as error:
            # Any exception will raise -> SQS retries / DLQ
            logger.error("Failed to process SQS message", exc_info=True)
            raise error

    logger.info("PDF Converter Lambda finished")

    return {
        "statusCode": 200,
        "message": "PDF conversion completed successfully"
    }
