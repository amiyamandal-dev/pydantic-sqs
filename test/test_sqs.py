import pytest
import boto3
from aiobotocore.session import get_session

from pydantic_sqs.model import SQSModel
from pydantic_sqs.queue import SQSQueueSync
from pydantic_sqs.exceptions import (
    MessageNotInQueueError,
    InvalidMessageInQueueError,
    ModelAlreadyRegisteredError,
)

# LocalStack endpoint
LOCALSTACK_ENDPOINT = "http://localhost:4566"
TEST_QUEUE_NAME = "test-queue"


@pytest.fixture
def sqs_client():
    """Fixture for synchronous SQS client"""
    return boto3.client("sqs", endpoint_url=LOCALSTACK_ENDPOINT, region_name="us-east-1")



@pytest.fixture
def test_queue_url(sqs_client):
    """Create a test queue and return its URL"""
    response = sqs_client.create_queue(QueueName=TEST_QUEUE_NAME)
    print(response["QueueUrl"])
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



class TestModel(SQSModel):
    """Test model for SQS operations"""
    name: str
    value: int



def test_sync_send_receive_delete(sync_queue, test_queue_url):
    """Test basic sync send, receive, delete operations"""
    # Register the model
    sync_queue.register_model(TestModel)

    # Create and send a message
    test_obj = TestModel(name="sync_test", value=24)
    test_obj.to_sqs_sync()

    assert test_obj.message_id is not None

    # Receive the message
    received = TestModel.from_sqs_sync(ignore_empty=False)
    assert len(received) == 1
    received_obj = received[0]

    assert received_obj.name == "sync_test"
    assert received_obj.value == 24
    assert received_obj.message_id == test_obj.message_id
    assert received_obj.receipt_handle is not None

    # Delete the message
    received_obj.delete_from_queue_sync()
    assert received_obj.deleted is True

    # Verify queue is empty
    with pytest.raises(Exception):
        TestModel.from_sqs_sync(ignore_empty=False)


def test_sync_batch_operations(sync_queue, test_queue_url):
    """Test sync batch send and delete"""
    sync_queue.register_model(TestModel)

    # Create multiple messages
    messages = [
        TestModel(name=f"sync_test_{i}", value=i)
        for i in range(5)
    ]

    # Send in batch
    sync_queue.send_messages_batch(messages)
    for msg in messages:
        assert msg.message_id is None  # Batch send doesn't set message_id

    # Receive messages
    received = TestModel.from_sqs_sync(max_messages=5, ignore_empty=False)
    assert len(received) == 5

    # Delete in batch
    result = sync_queue.delete_messages_batch(received)
    assert len(result.get("Successful", [])) == 5
    for msg in received:
        assert msg.deleted is True


def test_model_already_registered(sync_queue):
    """Test that registering a model twice raises an exception"""
    sync_queue.register_model(TestModel)
    with pytest.raises(ModelAlreadyRegisteredError):
        sync_queue.register_model(TestModel)


@pytest.mark.asyncio
async def test_invalid_message_handling(async_queue, test_queue_url, async_sqs_client):
    """Test handling of invalid messages"""
    # Send a raw invalid message directly to the queue
    await async_sqs_client.send_message(
        QueueUrl=test_queue_url,
        MessageBody="invalid message"
    )

    # Try to receive with ignore_unknown=False (should raise)
    with pytest.raises(InvalidMessageInQueueError):
        await TestModel.from_sqs(ignore_unknown=False)

    # Try with ignore_unknown=True (should not raise)
    results = await TestModel.from_sqs(ignore_unknown=True)
    assert len(results) == 0


def test_message_not_in_queue_error(sync_queue):
    """Test error when trying to delete message not in queue"""
    test_obj = TestModel(name="test", value=1)
    with pytest.raises(MessageNotInQueueError):
        test_obj.delete_from_queue_sync()


@pytest.mark.asyncio
async def test_msgpack_serialization(async_queue, test_queue_url):
    """Test message serialization with msgpack"""
    msgpack_queue = SQSQueue(
        queue_url=test_queue_url,
        aws_region="us-east-1",
        endpoint_url=LOCALSTACK_ENDPOINT,
        serializer="msgpack"
    )
    msgpack_queue.register_model(TestModel)

    test_obj = TestModel(name="msgpack_test", value=99)
    await test_obj.to_sqs()

    received = await TestModel.from_sqs(ignore_empty=False)
    assert len(received) == 1
    assert received[0].name == "msgpack_test"
    assert received[0].value == 99