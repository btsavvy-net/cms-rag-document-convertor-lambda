"""
ocr_service.py

Responsibilities:
- Send scanned page image to AI Gateway Lambda
- Receive structured OCR JSON response
- Return parsed elements
"""

import json
import base64
import boto3

from core.logging import setup_logger
from core.settings import settings

logger = setup_logger(__name__)


class OCRService:
    def __init__(self):
        # Lambda client to call AI Gateway Lambda
        self.lambda_client = boto3.client(
            "lambda",
            region_name=settings.AWS_REGION,
        )

        # AI Gateway Lambda ARN (must be configured in env)
        self.lambda_arn = settings.AI_GATEWAY_LAMBDA_ARN

        # Model name configured centrally
        self.model_name = "gpt-4o"

    # ===============================
    # OCR Extraction Function
    # ===============================
    def extract_text_from_image(self, image_bytes: bytes):
        """
        Sends image to AI Gateway Lambda
        Returns OCR extracted elements
        """

        logger.info("Invoking AI Gateway Lambda for OCR")

        # Base64 encode image
        base64_image = base64.b64encode(image_bytes).decode("utf-8")

        # Gateway-compatible request body
        request_body = {
            "model_provider": "openai",
            "model_name": self.model_name,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an OCR engine. Extract readable text from images."
                },
                {
                    "role": "user",
                    "content": "Extract all readable text from the provided image and return JSON format."
                }
            ],
            "image_base64": base64_image
        }

        # API Gateway style wrapper payload
        payload = {
            "version": "2.0",
            "routeKey": "POST /v1/chat/completions",
            "rawPath": "/v1/chat/completions",
            "headers": {
                "accept": "application/json",
                "content-type": "application/json",
            },
            "requestContext": {
                "http": {
                    "method": "POST",
                    "path": "/v1/chat/completions",
                }
            },
            "body": json.dumps(request_body),
            "isBase64Encoded": False,
        }

        try:
            response = self.lambda_client.invoke(
                FunctionName=self.lambda_arn,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )

            raw_payload = response["Payload"].read()
            result = json.loads(raw_payload)

        except Exception as e:
            logger.error(f"AI Gateway Lambda invocation failed: {str(e)}")
            raise RuntimeError("OCR Lambda invocation failed") from e

        # ===============================
        # Response Validation
        # ===============================
        status_code = result.get("statusCode")

        if status_code != 200:
            logger.error(f"AI Gateway returned non-200 status: {status_code}")
            logger.error(f"Full response: {result}")
            raise RuntimeError(f"OCR failed with status {status_code}")

        # ===============================
        # Parse OCR Response Body
        # ===============================
        try:
            body = json.loads(result["body"])

            content = body["choices"][0]["message"]["content"]

            parsed = json.loads(content)

            return parsed.get("elements", [])

        except Exception as parse_error:
            logger.error(
                f"Failed to parse OCR response: {str(parse_error)}"
            )
            raise RuntimeError("Invalid OCR response format") from parse_error
