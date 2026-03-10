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
import os
from typing import List, Dict

from litellm import completion
from core.logging import setup_logger
from core.settings import settings

logger = setup_logger("document_converter_lambda_ocr_service")


class OCRService:
    def __init__(self):
        """
        Initialize OCR service.

        - Uses AWS Secrets Manager for OpenRouter key
        - Uses LiteLLM for LLM calls
        - Uses DeepSeek V3.2 model
        """

        # Model configurable via environment variable
        self.model_name = os.getenv(
            "LLM_MODEL",
            "openrouter/deepseek/deepseek-v3.2"
        )

        # AWS Secrets Manager client
        self.secrets_client = boto3.client(
            "secretsmanager",
            region_name=settings.AWS_REGION,
        )

        # Secret name
        self.secret_name = os.getenv(
            "OPENAI_SECRET_NAME",
            "openrouter/contract/key"
        )

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
    # PDF OCR
    # =====================================================
    def _ocr_pdf(self, pdf_bytes: bytes) -> List[Dict]:

        logger.info("Processing scanned PDF for OCR")

        import fitz  # PyMuPDF

        elements = []

        try:
            doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            for page_index in range(len(doc)):

                logger.info(f"OCR processing page {page_index + 1}")

                page = doc.load_page(page_index)

                pix = page.get_pixmap(dpi=200)

                image_bytes = pix.tobytes("png")

                page_elements = self._send_to_gateway(
                    image_bytes=image_bytes,
                    mime_type="image/png"
                )

                if not page_elements:
                    logger.warning(
                        f"OCR returned empty text for page {page_index + 1}"
                    )

                elements.extend(page_elements)

        except Exception as e:
            logger.error(f"PDF OCR failed | error={str(e)}")

        return elements

    # =====================================================
    # IMAGE OCR
    # =====================================================
    def _ocr_image(self, image_bytes: bytes, filename: str) -> List[Dict]:

        logger.info("Processing image OCR")

        mime_type, _ = mimetypes.guess_type(filename)

        if not mime_type:
            mime_type = "image/png"

        elements = self._send_to_gateway(
            image_bytes=image_bytes,
            mime_type=mime_type
        )

        if not elements:
            logger.warning("OCR returned empty text for image")

        return elements

    # =====================================================
    # SECRET FETCHING
    # =====================================================
    def _get_openai_key(self) -> str:
        """
        Fetch OpenRouter API key from AWS Secrets Manager.
        Supports multiple secret formats.
        """

        try:

            logger.info("Fetching API key from Secrets Manager")

            response = self.secrets_client.get_secret_value(
                SecretId=self.secret_name
            )

            secret_string = response.get("SecretString")

            if not secret_string:
                raise ValueError(
                    f"Secret '{self.secret_name}' does not contain SecretString"
                )

            # Support JSON secrets
            try:

                secret_json = json.loads(secret_string)

                api_key = (
                    secret_json.get("OPENROUTER_API_KEY")
                    or secret_json.get("api_key")
                    or secret_json.get("key")
                )

                if api_key:
                    return api_key

            except json.JSONDecodeError:
                pass

            # Support plain text secrets
            return secret_string.strip()

        except Exception as e:
            logger.error(f"Failed to retrieve API key | error={str(e)}")
            raise

    # =====================================================
    # LLM OCR CALL
    # =====================================================
    def _send_to_gateway(self, image_bytes: bytes, mime_type: str) -> List[Dict]:
        """
        NOTE:
        Method name retained to avoid breaking other files.
        Internally calls DeepSeek via OpenRouter.
        """

        try:

            logger.info("Preparing image for OCR model")

            base64_image = base64.b64encode(image_bytes).decode("utf-8")

            api_key = self._get_openai_key()

            logger.info("Calling DeepSeek V3.2 via OpenRouter")

            response = completion(
                model=self.model_name,
                api_key=api_key,
                api_base="https://openrouter.ai/api/v1",
                temperature=0.0,
                max_tokens=4096,
                messages=[
                    {
                        "role": "system",
                        "content": (
                            "You are a strict OCR extraction engine.\n"
                            "Extract ALL readable document text.\n"
                            "Return ONLY valid JSON.\n"
                            "Do NOT explain.\n"
                            "Do NOT use markdown.\n"
                            'Output format: { "elements":[ {"text":"extracted text"} ] }\n'
                            'If no readable text return { "elements":[] }'
                        ),
                    },
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:{mime_type};base64,{base64_image}"
                                },
                            }
                        ],
                    },
                ],
            )

            content = response["choices"][0]["message"]["content"]

            if not content:
                logger.warning("Model returned empty content")
                return []

            content = content.strip()

            # Remove markdown if model adds it
            if content.startswith("```"):
                content = content.replace("```json", "")
                content = content.replace("```", "")
                content = content.strip()

            parsed = json.loads(content)

            if not isinstance(parsed, dict):
                logger.warning("OCR returned non-dict JSON")
                return []

            logger.info("OCR extraction successful")

            return parsed.get("elements", [])

        except Exception as e:

            logger.error(f"OCR LLM call failed | error={str(e)}")

            return []
