"""
sqs_notifier.py — SQS notification layer for ETL pipeline events.

After each ETL job writes output to S3, this module publishes a JSON
event to an SQS queue so downstream consumers (dashboards, alerts,
downstream pipelines) can react to new data without polling S3.

Usage
-----
    from jobs.sqs_notifier import notify_etl_complete
    notify_etl_complete(queue_url, s3_uri, row_count)

Queue bootstrap (first run)
---------------------------
Call `ensure_queue` once at startup to create the queue if it does
not already exist — idempotent, safe to call every run.

    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = ensure_queue(sqs, queue_name="etl-events")
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import boto3

logger = logging.getLogger(__name__)


def ensure_queue(sqs_client, queue_name: str) -> str:
    """
    Create the SQS queue if it does not exist and return its URL.
    Equivalent to boto3 create_queue — safe to call on every startup.
    """
    response = sqs_client.create_queue(
        QueueName=queue_name,
        Attributes={
            "MessageRetentionPeriod": "86400",   # 1 day
            "VisibilityTimeout": "60",
        },
    )
    queue_url: str = response["QueueUrl"]
    logger.info("SQS queue ready: %s", queue_url)
    return queue_url


def notify_etl_complete(
    queue_url: str,
    s3_uri: str,
    row_count: int,
    job_name: str = "etl_api_to_csv_products",
    region: str = "us-east-1",
) -> str:
    """
    Publish an ETL-completion event to the SQS queue.

    Returns the SQS MessageId on success.
    """
    sqs = boto3.client("sqs", region_name=region)

    payload = {
        "event": "etl_complete",
        "job_name": job_name,
        "s3_output": s3_uri,
        "row_count": row_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(payload),
        MessageAttributes={
            "job_name": {
                "DataType": "String",
                "StringValue": job_name,
            }
        },
    )

    message_id: str = response["MessageId"]
    logger.info("SQS event published — MessageId=%s queue=%s", message_id, queue_url)
    return message_id
