import boto3
import pytest
from aiobotocore.session import get_session

from pydantic_sqs.model import SQSModel
from pydantic_sqs.queue import SQSQueueSync


class MyModel(SQSModel):
    """Test model for SQS operations"""
    foo: str
    bar: int


LOCALSTACK_ENDPOINT = "http://localhost:4566"
TEST_QUEUE_NAME = "test-queue"


@pytest.fixture
def sqs_client():
    """Fixture for synchronous SQS client"""
    return boto3.client("sqs", endpoint_url=LOCALSTACK_ENDPOINT, region_name="us-east-1")


@pytest.fixture
async def async_sqs_client():
    """Fixture for asynchronous SQS client"""
    session = get_session()
    async with session.create_client("sqs", endpoint_url=LOCALSTACK_ENDPOINT, region_name="us-east-1") as client:
        yield client


@pytest.fixture
def test_queue_url(sqs_client):
    """Create a test queue and return its URL"""
    response = sqs_client.create_queue(QueueName=TEST_QUEUE_NAME)
    return response["QueueUrl"]


@pytest.fixture
def sync_queue(test_queue_url):
    """Fixture for synchronous queue"""
    return SQSQueueSync(
        queue_url=test_queue_url,
        aws_region="us-east-1",
        endpoint_url=LOCALSTACK_ENDPOINT,
        serializer="json"
    )


def test_sync_workflow(sync_queue):
    """Test the complete synchronous workflow from the example"""
    # Register the model
    sync_queue.register_model(MyModel)

    # Create a model instance
    test_message = "Hello from pytest"
    test_value = 42
    model_instance = MyModel(foo=test_message, bar=test_value)

    # Send to SQS
    model_instance.to_sqs_sync()
    assert model_instance.message_id is not None
    print(f"Sent message with ID: {model_instance.message_id}")

    # Receive messages
    received_models = sync_queue.from_sqs_sync(
        max_messages=1,
        ignore_empty=True
    )
    assert len(received_models) == 1

    # Verify received message
    received_model = received_models[0]
    print(f"Got model from SQS: {received_model}")
    assert received_model.foo == test_message
    assert received_model.bar == test_value
    assert received_model.message_id == model_instance.message_id

    # Delete from queue
    received_model.delete_from_queue_sync()
    assert received_model.deleted is True

    # Verify queue is empty
    with pytest.raises(Exception):
        sync_queue.from_sqs_sync(ignore_empty=False)


def test_multiple_messages(sync_queue):
    """Test sending and receiving multiple messages"""
    sync_queue.register_model(MyModel)

    # Send multiple messages
    messages = [
        MyModel(foo=f"Message {i}", bar=i)
        for i in range(3)
    ]
    for msg in messages:
        msg.to_sqs_sync()

    # Receive all messages
    received = sync_queue.from_sqs_sync(max_messages=10, ignore_empty=True)
    assert len(received) == 3

    # Verify contents
    for i, msg in enumerate(sorted(received, key=lambda x: x.bar)):
        assert msg.foo == f"Message {i}"
        assert msg.bar == i

    # Clean up
    for msg in received:
        msg.delete_from_queue_sync()


def test_message_not_found(sync_queue):
    """Test handling of empty queue"""
    sync_queue.register_model(MyModel)

    # Should raise when ignore_empty=False
    with pytest.raises(Exception):
        sync_queue.from_sqs_sync(ignore_empty=False)

    # Should return empty list when ignore_empty=True
    result = sync_queue.from_sqs_sync(ignore_empty=True)
    assert len(result) == 0
