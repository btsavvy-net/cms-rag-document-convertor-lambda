"""
ocr_service.py

Production OCR Service Supporting:

- PDF scanned document OCR
- JPG / JPEG / PNG OCR
- File type auto detection
- IR compatible output
"""

import json
import base64
import boto3
import mimetypes

import fitz  # PyMuPDF

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
    # Public OCR Entry Function
    # =========================================================
    def extract_text(self, file_bytes: bytes, filename: str):
        """
        Auto detect file type and perform OCR.

        Supports:
        - PDF
        - JPG
        - JPEG
        - PNG
        """

        logger.info(f"OCR Processing started | file={filename}")

        file_type = self._detect_file_type(filename)

        if file_type == "pdf":
            return self._ocr_pdf(file_bytes)

        elif file_type in ["jpg", "jpeg", "png"]:
            return self._ocr_image(file_bytes)

        else:
            logger.error(f"Unsupported file type: {filename}")
            return []

    # =========================================================
    # File Type Detection
    # =========================================================
    def _detect_file_type(self, filename: str):
        mime, _ = mimetypes.guess_type(filename)

        if mime == "application/pdf":
            return "pdf"

        if mime and mime.startswith("image"):
            return filename.split(".")[-1].lower()

        return "unknown"

    # =========================================================
    # PDF OCR → Convert Each Page To Image
    # =========================================================
    def _ocr_pdf(self, pdf_bytes: bytes):
        logger.info("Processing scanned PDF OCR")

        elements = []

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            for page_index in range(len(doc)):
                page = doc.load_page(page_index)

                pix = page.get_pixmap(dpi=200)
                image_bytes = pix.tobytes("png")

                page_elements = self._send_to_gateway(image_bytes)

                elements.extend(page_elements)

        except Exception as e:
            logger.error(f"PDF OCR failed: {str(e)}")

        return elements

    # =========================================================
    # Image OCR
    # =========================================================
    def _ocr_image(self, image_bytes: bytes):
        logger.info("Processing image OCR")

        return self._send_to_gateway(image_bytes)

    # =========================================================
    # Gateway Invocation Layer
    # =========================================================
    def _send_to_gateway(self, image_bytes: bytes):
        base64_image = base64.b64encode(image_bytes).decode("utf-8")

        request_body = {
            "model_provider": "openai",
            "model_name": self.model_name,
            "temperature": 0.0,
            "max_tokens": 4096,
            "messages": [
                {
                    "role": "system",
                    "content": """
You are a strict OCR extraction engine.

RULES:
- Extract ALL readable text from image.
- Return ONLY JSON.
- Do NOT explain.
- Do NOT return markdown.

Output format:

{
  "elements":[
    {"text":"extracted text"}
  ]
}

If no text exists return:

{ "elements":[] }
"""
                },
                {
                    "role": "user",
                    "content": "Perform OCR extraction."
                }
            ],
            "image_base64": base64_image
        }

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

            if not raw_payload:
                return []

            result = json.loads(raw_payload)

            if result.get("statusCode") != 200:
                return []

            body = json.loads(result.get("body", "{}"))

            choices = body.get("choices", [])

            if not choices:
                return []

            content = choices[0].get("message", {}).get("content", "").strip()

            if not content:
                return []

            if content.startswith("```"):
                content = content.replace("```json", "")
                content = content.replace("```", "")
                content = content.strip()

            parsed = json.loads(content)

            if not isinstance(parsed, dict):
                return []

            return parsed.get("elements", [])

        except Exception as e:
            logger.error(f"OCR gateway call failed: {str(e)}")
            return []
