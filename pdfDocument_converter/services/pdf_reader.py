"""
pdf_reader.py

Responsibilities:
- Open a PDF
- Read text page by page
- If native text exists → use normal extraction
- If page is scanned (no text layer) → run OCR using OpenAI
- Return IR-compliant elements
"""

import fitz  # PyMuPDF
import hashlib
from datetime import datetime, timezone

from core.logging import setup_logger
from services.ocr_service import OCRService  # ✅ OCR integration

logger = setup_logger(__name__)

# Initialize OCR service once (reuse across pages)
ocr_service = OCRService()


def generate_hash(text: str) -> str:
    """
    Generate a stable SHA-256 hash for the text.
    Used for deduplication and chunk tracking.
    """
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def read_pdf(pdf_bytes: bytes, tenant_id: str, doc_id: str):
    """
    Extracts text from a PDF and returns IR elements.

    Hybrid Logic:
    - If page has native text → extract normally
    - If page has no native text → run OCR
    """

    logger.info("Opening PDF document")

    try:
        pdf_document = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception as e:
        logger.error(f"Failed to open PDF: {str(e)}")
        raise

    extracted_elements = []
    element_counter = 1

    total_pages = pdf_document.page_count
    logger.info(f"Total pages in PDF: {total_pages}")

    for page_index in range(total_pages):
        try:
            page = pdf_document.load_page(page_index)
            page_number = page_index + 1

            logger.info(f"Processing page {page_number}")

            # Try extracting native text blocks
            text_blocks = page.get_text("blocks")

            # Check if page contains real text
            has_native_text = any(block[4].strip() for block in text_blocks)

            # ============================
            # CASE 1: Native PDF Page
            # ============================
            if has_native_text:
                logger.info(f"Page {page_number} detected as native text page")

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

            # ============================
            # CASE 2: Scanned Page → OCR
            # ============================
            else:
                logger.warning(
                    f"Page {page_number} detected as scanned page. Running OCR..."
                )

                try:
                    # Convert page to image
                    pix = page.get_pixmap(dpi=200)
                    image_bytes = pix.tobytes("png")

                    # Call OCR service
                    ocr_results = ocr_service.extract_text_from_image(image_bytes)

                    if not ocr_results:
                        logger.warning(
                            f"OCR returned no text for page {page_number}"
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
                            "bbox": None,  # OCR pages do not have bbox
                            "lang": "en",
                            "table_html": None,
                            "hash": generate_hash(text),
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }

                        extracted_elements.append(element)
                        element_counter += 1

                    logger.info(
                        f"OCR completed successfully for page {page_number}"
                    )

                except Exception as ocr_error:
                    logger.error(
                        f"OCR failed for page {page_number}: {str(ocr_error)}"
                    )
                    raise

        except Exception as page_error:
            logger.error(
                f"Error processing page {page_index + 1}: {str(page_error)}"
            )
            raise

    logger.info(f"Total extracted elements: {len(extracted_elements)}")

    return extracted_elements
