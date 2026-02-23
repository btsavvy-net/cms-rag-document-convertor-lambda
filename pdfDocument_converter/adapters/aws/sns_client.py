# sns_client.py

"""
SNSClient

Low-level SNS publisher.

Responsibilities:
- Publish messages to SNS topics
- Encapsulate boto3 SNS operations
- NO business logic
- NO payload construction rules
"""

import json
import boto3

from core.settings import settings
from core.logging import setup_logger

logger = setup_logger(__name__)

_sns_client = None


def get_sns_client():
    """
    Lazy SNS client creation.
    Reused across Lambda invocations.
    """
    global _sns_client
    if _sns_client is None:
        _sns_client = boto3.client(
            "sns",
            region_name=settings.AWS_REGION
        )
    return _sns_client


class SNSClient:
    def publish(
        self,
        topic_arn: str,
        message_body: dict,
        attributes: dict | None = None,
    ):
        """
        Publish a message to SNS.

        Parameters:
        - topic_arn: SNS topic ARN
        - message_body: JSON-serializable dict
        - attributes: optional SNS MessageAttributes
        """

        if not topic_arn:
            logger.warning("SNS topic ARN is empty. Skipping publish.")
            return None

        message_attributes = {}
        if attributes:
            message_attributes = {
                key: {
                    "DataType": "String",
                    "StringValue": str(value),
                }
                for key, value in attributes.items()
            }

        logger.info(f"Publishing message to SNS topic: {topic_arn}")

        response = get_sns_client().publish(
            TopicArn=topic_arn,
            Message=json.dumps(message_body),
            MessageAttributes=message_attributes,
        )

        logger.info(
            f"SNS publish successful. MessageId={response.get('MessageId')}"
        )

        return response
