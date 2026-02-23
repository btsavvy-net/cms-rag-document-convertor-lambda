"""
dynamodb_repo.py
----------------

Low-level DynamoDB repository for the `rag-docs` table.

Responsibilities:
- Perform direct DynamoDB UpdateItem calls
- Use typed DynamoDB attribute syntax (S, N, M)
- NO business logic (no PDF / SNS decisions)

This layer is intentionally dumb.
"""

import boto3
from datetime import datetime, timezone


class DynamoDBRepository:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.ddb = boto3.client("dynamodb")

    def update_conversion_status(
        self,
        tenant_id: str,
        doc_id: str,
        ir_s3_key: str,
        page_count: int,
        element_count: int,
        table_count: int,
        schema_version: str,
        pymupdf_version: str = "1.24.8",
        camelot_version: str = "0.11.0",
        timings_ms: dict | None = None,
    ):
        """
        Update PDF conversion details in DynamoDB.

        Table keys:
        - tenant_id (PK)
        - doc_id    (SK)  -> value must be "DOC#{doc_id}"
        """

        if timings_ms is None:
            timings_ms = {}

        now = datetime.now(timezone.utc).isoformat()

        self.ddb.update_item(
            TableName=self.table_name,
            Key={
                "tenant_id": {"S": tenant_id},
                "doc_id": {"S": f"DOC#{doc_id}"},
            },
            UpdateExpression="""
                SET
                    #status = :status,
                    conversion = :conversion,
                    updated_at = :updated_at
            """,
            ExpressionAttributeNames={
                "#status": "status",
            },
            ExpressionAttributeValues={
                ":status": {"S": "converted"},
                ":updated_at": {"S": now},
                ":conversion": {
                    "M": {
                        "stage": {"S": "pdf"},
                        "ir_s3_key": {"S": ir_s3_key},
                        "page_count": {"N": str(page_count)},
                        "element_count": {"N": str(element_count)},
                        "table_count": {"N": str(table_count)},
                        "extractor": {
                            "M": {
                                "pymupdf": {"S": pymupdf_version},
                                "camelot": {"S": camelot_version},
                            }
                        },
                        "timings_ms": {
                            "M": {
                                k: {"N": str(v)} for k, v in timings_ms.items()
                            }
                        },
                        "schema_version": {"S": schema_version},
                    }
                },
            },
        )
