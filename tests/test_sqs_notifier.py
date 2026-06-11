import json

import boto3
import pytest
from moto import mock_aws

from src.jobs.sqs_notifier import ensure_queue, notify_etl_complete


# ═════════════════════════════════════════════════════════════════════════════
# 1 ▸ ensure_queue
# ═════════════════════════════════════════════════════════════════════════════

@mock_aws
def test_ensure_queue_creates_and_returns_url():
    sqs = boto3.client("sqs", region_name="us-east-1")
    url = ensure_queue(sqs, "etl-events")
    assert "etl-events" in url


@mock_aws
def test_ensure_queue_is_idempotent():
    sqs = boto3.client("sqs", region_name="us-east-1")
    url1 = ensure_queue(sqs, "etl-events")
    url2 = ensure_queue(sqs, "etl-events")
    assert url1 == url2


# ═════════════════════════════════════════════════════════════════════════════
# 2 ▸ notify_etl_complete
# ═════════════════════════════════════════════════════════════════════════════

@mock_aws
def test_notify_etl_complete_returns_message_id():
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = ensure_queue(sqs, "etl-events")

    message_id = notify_etl_complete(
        queue_url=queue_url,
        s3_uri="s3://my-bucket/products/processed/catalog.csv",
        row_count=194,
        region="us-east-1",
    )

    assert isinstance(message_id, str)
    assert len(message_id) > 0


@mock_aws
def test_notify_etl_complete_message_body():
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = ensure_queue(sqs, "etl-events")

    notify_etl_complete(
        queue_url=queue_url,
        s3_uri="s3://my-bucket/products/processed/catalog.csv",
        row_count=42,
        job_name="test_job",
        region="us-east-1",
    )

    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=1)
    body = json.loads(messages["Messages"][0]["Body"])

    assert body["event"] == "etl_complete"
    assert body["job_name"] == "test_job"
    assert body["s3_output"] == "s3://my-bucket/products/processed/catalog.csv"
    assert body["row_count"] == 42
    assert "timestamp" in body


@mock_aws
def test_notify_etl_complete_message_attributes():
    sqs = boto3.client("sqs", region_name="us-east-1")
    queue_url = ensure_queue(sqs, "etl-events")

    notify_etl_complete(
        queue_url=queue_url,
        s3_uri="s3://bucket/key.csv",
        row_count=10,
        job_name="my_etl",
        region="us-east-1",
    )

    messages = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        MessageAttributeNames=["All"],
    )
    attrs = messages["Messages"][0]["MessageAttributes"]
    assert attrs["job_name"]["StringValue"] == "my_etl"
