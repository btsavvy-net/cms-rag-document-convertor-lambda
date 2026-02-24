"""
ocr_service.py

Production OCR Service

Supports:
- PDF scanned document OCR
- JPG / JPEG / PNG OCR
- Auto file type detection
- IR compatible output
"""

import json
import base64
import boto3
import mimetypes
from typing import List, Dict

from core.logging import setup_logger
from core.settings import settings

logger = setup_logger("document_converter_lambda_ocr_service")


class OCRService:
    def __init__(self):
        self.lambda_client = boto3.client(
            "lambda",
            region_name=settings.AWS_REGION,
        )

        self.lambda_arn = settings.AI_GATEWAY_LAMBDA_ARN
        self.model_name = "gpt-4o"  # Vision capable model

    # =====================================================
    # PUBLIC ENTRY FUNCTION
    # =====================================================
    def extract_text(self, file_bytes: bytes, filename: str) -> List[Dict]:
        """
        Auto detect document type and perform OCR.

        Supports:
        - PDF
        - JPG
        - JPEG
        - PNG
        """

        logger.info(f"Document OCR processing started | file={filename}")

        file_type = self._detect_file_type(filename)

        if file_type == "pdf":
            return self._ocr_pdf(file_bytes)

        if file_type in ["jpg", "jpeg", "png"]:
            return self._ocr_image(file_bytes, filename)

        logger.error(f"Unsupported document format | filename={filename}")
        return []

    # =====================================================
    # File Type Detection
    # =====================================================
    def _detect_file_type(self, filename: str) -> str:
        mime, _ = mimetypes.guess_type(filename)

        if mime == "application/pdf":
            return "pdf"

        if mime and mime.startswith("image"):
            return filename.split(".")[-1].lower()

        return "unknown"

    # =====================================================
    # PDF OCR → Convert Each Page To Image
    # =====================================================
    def _ocr_pdf(self, pdf_bytes: bytes) -> List[Dict]:
        logger.info("Document converter lambda processing scanned PDF")

        import fitz  # PyMuPDF

        elements = []

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            for page_index in range(len(doc)):
                page = doc.load_page(page_index)

                pix = page.get_pixmap(dpi=200)
                image_bytes = pix.tobytes("png")

                page_elements = self._send_to_gateway(
                    image_bytes=image_bytes,
                    mime_type="image/png"
                )

                if not page_elements:
                    logger.warning(
                        f"Document OCR returned empty text for page {page_index + 1}"
                    )

                elements.extend(page_elements)

        except Exception as e:
            logger.error(f"PDF OCR failed | error={str(e)}")

        return elements

    # =====================================================
    # Image OCR
    # =====================================================
    def _ocr_image(self, image_bytes: bytes, filename: str) -> List[Dict]:
        logger.info("Document converter lambda processing image OCR")

        mime_type, _ = mimetypes.guess_type(filename)

        if not mime_type:
            mime_type = "image/png"

        elements = self._send_to_gateway(
            image_bytes=image_bytes,
            mime_type=mime_type
        )

        if not elements:
            logger.warning("Document OCR returned empty text for image")

        return elements

    # =====================================================
    # Gateway Invocation
    # =====================================================
    def _send_to_gateway(self, image_bytes: bytes, mime_type: str) -> List[Dict]:

        base64_image = base64.b64encode(image_bytes).decode("utf-8")

        request_body = {
            "model_provider": "openai",
            "model_name": self.model_name,
            "temperature": 0.0,
            "max_tokens": 4096,
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "You are a strict OCR extraction engine.\n"
                        "Extract ALL readable document text.\n"
                        "Return ONLY valid JSON.\n"
                        "Do NOT explain.\n"
                        "Do NOT use markdown.\n"
                        "Output format:\n"
                        '{ "elements":[ {"text":"extracted text"} ] }\n'
                        'If no readable text return { "elements":[] }'
                    ),
                },
                {
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Perform OCR extraction."},
                        {
                            "type": "image_url",
                            "image_url": {
                                "url": f"data:{mime_type};base64,{base64_image}"
                            },
                        },
                    ],
                },
            ],
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
                logger.warning("Document OCR gateway returned empty response")
                return []

            result = json.loads(raw_payload)

            if result.get("statusCode") != 200:
                logger.error(
                    f"Document OCR gateway non-200 status | {result.get('statusCode')}"
                )
                return []

            body = json.loads(result.get("body", "{}"))
            choices = body.get("choices", [])

            if not choices:
                logger.warning("Document OCR gateway returned no choices")
                return []

            content = choices[0].get("message", {}).get("content", "")

            if not content:
                logger.warning("Document OCR model returned empty content")
                return []

            content = content.strip()

            # Remove markdown fences if model adds them
            if content.startswith("```"):
                content = content.replace("```json", "")
                content = content.replace("```", "")
                content = content.strip()

            parsed = json.loads(content)

            if not isinstance(parsed, dict):
                logger.warning("Document OCR returned non-dict JSON")
                return []

            return parsed.get("elements", [])

        except Exception as e:
            logger.error(f"Document OCR gateway call failed | error={str(e)}")
            return []
