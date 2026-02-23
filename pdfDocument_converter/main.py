"""
main.py

This is the AWS Lambda ENTRY POINT.

Terraform / AWS Lambda will call:
    main.handler

This file only forwards the request to the real handler.
No business logic should live here.
"""

# Import the real Lambda handler
from app.lambda_.handler import handler as lambda_handler



def handler(event, context):
    """
    AWS Lambda calls this function first.

    event   -> SQS/SNS message
    context -> Lambda runtime details

    We simply pass both to the actual handler.
    """
    return lambda_handler(event, context)
