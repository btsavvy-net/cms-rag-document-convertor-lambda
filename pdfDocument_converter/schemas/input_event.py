# src/service_name/schemas/input_event.py

from pydantic import BaseModel


class InputEvent(BaseModel):
    """
    This schema represents the input event
    that triggers the PDF converter Lambda.

    It tells us:
    - Where the PDF is stored
    - Which tenant owns it
    - Which document it is
    """

    bucket: str
    key: str
    tenant_id: str
    doc_id: str
