# src/service_name/schemas/ir_element.py

from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class BoundingBox(BaseModel):
    """
    Bounding box coordinates of the text block in the page/slide.
    """
    x0: float
    y0: float
    x1: float
    y1: float

class IRElement(BaseModel):
    """
    Represents ONE extracted element from a document (PDF / PPT).

    Examples:
    - Paragraph
    - Heading
    - Table

    Required fields:
    - tenant_id, doc_id, element_id, type, hash, created_at, lang
    Optional fields can be None if not applicable.
    """

    tenant_id: str
    doc_id: str

    element_id: str
    type: str  # "paragraph" | "heading" | "table"

    text: Optional[str] = None
    table_html: Optional[str] = None

    page: Optional[int] = None
    slide: Optional[int] = None

    bbox: Optional[BoundingBox] = None
    lang: str  # ✅ Required, e.g., "en"

    hash: str  # ✅ Required SHA-256 hash of text
    created_at: datetime
