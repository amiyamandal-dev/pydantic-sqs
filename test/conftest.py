import pytest
import boto3
from moto import mock_aws as mock_sqs

from pydantic_sqs.queue import SQSQueue, SQSQueueSync


@pytest.fixture(scope="function")
@mock_sqs
def sync_queue_fixture():
    """
    Synchronous fixture using moto + boto3.
    """
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    queue_name = "test-sync-queue"
    create_resp = sqs_client.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]

    sync_queue = SQSQueueSync(
        queue_url=queue_url,
        aws_region="us-east-1",
        use_ssl=False,
        serializer="json",
    )
    yield sync_queue


@pytest.fixture(scope="function")
@mock_sqs
async def async_queue_fixture():
    """
    Asynchronous fixture using moto + boto3 + aiobotocore.
    """
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    queue_name = "test-async-queue"
    create_resp = sqs_client.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]

    async_queue = SQSQueue(
        queue_url=queue_url,
        aws_region="us-east-1",
        use_ssl=False,
        serializer="json",
    )

    async with async_queue:
        yield async_queue