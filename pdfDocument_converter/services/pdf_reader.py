"""
pdf_reader.py

Document Converter Lambda Reader Engine

Responsibilities:
- Read document (PDF scanned / native)
- Extract native text when available
- Run OCR when scanned document detected
- Generate IR compatible elements
"""

import fitz
import hashlib
from datetime import datetime, timezone

from core.logging import setup_logger
from services.ocr_service import OCRService

logger = setup_logger("document_converter_lambda_pdf_reader")

# OCR Service Singleton
ocr_service = OCRService()


# ======================================================
# Utility Functions
# ======================================================

def generate_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def is_native_page(page) -> bool:
    """
    Detect native document page using word-level analysis.
    Prevents false OCR detection.
    """

    words = page.get_text("words")

    if not words:
        return False

    total_chars = sum(len(w[4]) for w in words if w[4].strip())

    logger.debug(
        f"Document page analysis | words={len(words)} | chars={total_chars}"
    )

    return total_chars > 100


# ======================================================
# Main Document Reader
# ======================================================

def read_pdf(pdf_bytes: bytes, tenant_id: str, doc_id: str):
    """
    Hybrid Document Reader

    Supports:
    - Native PDF text extraction
    - OCR scanning pipeline
    - JPG / PNG document ingestion (via OCR service)
    """

    logger.info("Opening document inside document converter lambda")

    try:
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.error(f"Document open failed | error={str(e)}")
        raise

    extracted_elements = []
    element_counter = 1

    total_pages = pdf_document.page_count
    logger.info(f"Total pages in document: {total_pages}")

    for page_index in range(total_pages):
        try:
            page = pdf_document.load_page(page_index)
            page_number = page_index + 1

            logger.info(f"Processing document page {page_number}")

            has_native_text = is_native_page(page)

            # ==================================================
            # Native Document Page
            # ==================================================
            if has_native_text:

                logger.info(
                    f"Document page {page_number} detected as native text page"
                )

                text_blocks = page.get_text("blocks")

                for block in text_blocks:
                    text = block[4].strip()

                    if not text:
                        continue

                    element = {
                        "tenant_id": tenant_id,
                        "doc_id": doc_id,
                        "element_id": f"p{page_number}_{element_counter:03d}",
                        "type": "paragraph",
                        "text": text,
                        "page": page_number,
                        "slide": None,
                        "bbox": {
                            "x0": block[0],
                            "y0": block[1],
                            "x1": block[2],
                            "y1": block[3],
                        },
                        "lang": "en",
                        "table_html": None,
                        "hash": generate_hash(text),
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }

                    extracted_elements.append(element)
                    element_counter += 1

            # ==================================================
            # Scanned Document Page → OCR Pipeline
            # ==================================================
            else:

                logger.warning(
                    f"Document page {page_number} detected as scanned page. Running OCR..."
                )

                try:
                    pix = page.get_pixmap(dpi=200)
                    image_bytes = pix.tobytes("png")

                    # ⭐ Correct OCR Service Call
                    ocr_results = ocr_service.extract_text(
                        image_bytes,
                        filename="document_page.png"
                    )

                    if not ocr_results:
                        logger.warning(
                            f"OCR returned empty document text for page {page_number}"
                        )
                        continue

                    for ocr_block in ocr_results:

                        text = ocr_block.get("text", "").strip()

                        if not text:
                            continue

                        element = {
                            "tenant_id": tenant_id,
                            "doc_id": doc_id,
                            "element_id": f"p{page_number}_{element_counter:03d}",
                            "type": "paragraph",
                            "text": text,
                            "page": page_number,
                            "slide": None,
                            "bbox": None,
                            "lang": "en",
                            "table_html": None,
                            "hash": generate_hash(text),
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }

                        extracted_elements.append(element)
                        element_counter += 1

                    logger.info(
                        f"Document OCR completed for page {page_number}"
                    )

                except Exception as ocr_error:
                    logger.error(
                        f"Document OCR failed | page={page_number} | error={str(ocr_error)}"
                    )
                    raise

        except Exception as page_error:
            logger.error(
                f"Document processing error | page={page_index + 1} | error={str(page_error)}"
            )
            raise

    pdf_document.close()

    logger.info(
        f"Total document IR elements extracted: {len(extracted_elements)}"
    )

    return extracted_elements
