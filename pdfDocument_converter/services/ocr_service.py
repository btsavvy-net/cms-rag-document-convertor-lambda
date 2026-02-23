"""
ocr_service.py

Production Safe OCR Service
"""

import json
import base64
import boto3

from core.logging import setup_logger
from core.settings import settings

logger = setup_logger(__name__)


class OCRService:
    def __init__(self):
        self.lambda_client = boto3.client(
            "lambda",
            region_name=settings.AWS_REGION,
        )

        self.lambda_arn = settings.AI_GATEWAY_LAMBDA_ARN

        self.model_name = "gpt-4o"

    # =========================================================
    # OCR Extraction Function (Production Safe Version)
    # =========================================================
    def extract_text_from_image(self, image_bytes: bytes):
        """
        Sends image to AI Gateway Lambda
        Returns OCR extracted elements safely
        """

        logger.info("Invoking AI Gateway Lambda for OCR")

        base64_image = base64.b64encode(image_bytes).decode("utf-8")

        # =====================================================
        # STRICT OCR PROMPT
        # =====================================================
        request_body = {
            "model_provider": "openai",
            "model_name": self.model_name,
            "temperature": 0.0,
            "messages": [
                {
                    "role": "system",
                    "content": """
You are a strict OCR extraction engine.

RULES:
- Extract ALL readable text from image.
- Return ONLY JSON.
- DO NOT explain.
- DO NOT return markdown.
- If no text exists, return exactly:

{ "elements": [] }

Output format MUST be:

{
  "elements":[
    {
      "text":"extracted text"
    }
  ]
}
"""
                },
                {
                    "role": "user",
                    "content": "Perform OCR extraction on the provided image."
                }
            ],
            "image_base64": base64_image
        }

        # Gateway wrapper payload
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

        # =====================================================
        # Invoke Gateway Lambda
        # =====================================================
        try:
            response = self.lambda_client.invoke(
                FunctionName=self.lambda_arn,
                InvocationType="RequestResponse",
                Payload=json.dumps(payload),
            )

            raw_payload = response["Payload"].read()

            if not raw_payload:
                logger.warning("Empty OCR gateway response")
                return []

            result = json.loads(raw_payload)

        except Exception as e:
            logger.error(f"AI Gateway Lambda invocation failed: {str(e)}")
            return []

        # =====================================================
        # Response Validation
        # =====================================================
        status_code = result.get("statusCode")

        if status_code != 200:
            logger.error(f"AI Gateway returned non-200 status: {status_code}")
            return []

        # =====================================================
        # Safe Response Parsing
        # =====================================================
        try:
            body = json.loads(result.get("body", "{}"))

            choices = body.get("choices", [])

            if not choices:
                logger.warning("OCR response choices missing")
                return []

            content = choices[0].get("message", {}).get("content", "").strip()

            if not content:
                logger.warning("OCR response content empty")
                return []

            # Remove markdown fences if model adds them
            if content.startswith("```"):
                content = content.replace("```json", "")
                content = content.replace("```", "")
                content = content.strip()

            parsed = json.loads(content)

            if not isinstance(parsed, dict):
                logger.warning("OCR response is not JSON object")
                return []

            return parsed.get("elements", [])

        except json.JSONDecodeError:
            logger.error("OCR raw content invalid JSON")
            return []

        except Exception as parse_error:
            logger.error(f"Failed to parse OCR response: {str(parse_error)}")
            return []
