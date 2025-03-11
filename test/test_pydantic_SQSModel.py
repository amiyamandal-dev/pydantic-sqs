import pytest

from pydantic_sqs.model import SQSModel
from pydantic_sqs import exceptions


class SimpleModel(SQSModel):
    """
    Minimal Pydantic model for demonstration.
    """
    text: str


@pytest.mark.asyncio
async def test_async_model_send_receive(async_queue_fixture):
    """
    Test sending and receiving a single message via the async SQSQueue.
    """
    queue = async_queue_fixture
    queue.register_model(SimpleModel)

    model_instance = SimpleModel(text="hello-async")
    await model_instance.to_sqs()
    assert model_instance.message_id is not None

    results = await queue.from_sqs()
    assert len(results) == 1
    msg = results[0]
    assert msg.text == "hello-async"
    assert msg.message_id == model_instance.message_id
    assert msg.receipt_handle is not None
    assert not msg.deleted

    await msg.delete_from_queue()
    assert msg.deleted


def test_sync_model_send_receive(sync_queue_fixture):
    """
    Test sending and receiving a single message via the sync SQSQueueSync.
    """
    queue = sync_queue_fixture
    queue.register_model(SimpleModel)

    model_instance = SimpleModel(text="hello-sync")
    model_instance.to_sqs_sync()
    assert model_instance.message_id is not None

    results = queue.from_sqs_sync()
    assert len(results) == 1
    msg = results[0]
    assert msg.text == "hello-sync"
    assert msg.message_id == model_instance.message_id
    assert msg.receipt_handle is not None
    assert not msg.deleted

    msg.delete_from_queue_sync()
    assert msg.deleted


@pytest.mark.asyncio
async def test_async_batch_send_delete(async_queue_fixture):
    """
    Test batch sending and deleting using the async SQSQueue.
    """
    queue = async_queue_fixture
    queue.register_model(SimpleModel)

    models = [SimpleModel(text=f"batch-item-{i}") for i in range(3)]
    await queue.send_messages_batch(models)

    results = await queue.from_sqs(max_messages=3)
    assert len(results) == 3
    texts = sorted(r.text for r in results)
    assert texts == ["batch-item-0", "batch-item-1", "batch-item-2"]

    await queue.delete_messages_batch(results)
    empty_results = await queue.from_sqs(ignore_empty=True)
    assert len(empty_results) == 0


def test_sync_batch_send_delete(sync_queue_fixture):
    """
    Test batch sending and deleting using the sync SQSQueueSync.
    """
    queue = sync_queue_fixture
    queue.register_model(SimpleModel)

    models = [SimpleModel(text=f"sync-batch-{i}") for i in range(3)]
    queue.send_messages_batch(models)

    results = queue.from_sqs_sync(max_messages=3)
    assert len(results) == 3
    texts = sorted(r.text for r in results)
    assert texts == ["sync-batch-0", "sync-batch-1", "sync-batch-2"]

    queue.delete_messages_batch(results)
    assert len(queue.from_sqs_sync(ignore_empty=True)) == 0


def test_not_registered_error(sync_queue_fixture):
    """
    Attempting to send a model that isn't registered should raise NotRegisteredError.
    """
    class UnregisteredModel(SQSModel):
        foo: str

    unreg_instance = UnregisteredModel(foo="nope")
    with pytest.raises(exceptions.NotRegisteredError):
        unreg_instance.to_sqs_sync()


@pytest.mark.asyncio
async def test_delete_not_in_queue(async_queue_fixture):
    """
    Attempting to delete a model that has no receipt_handle should raise.
    """
    class NoQueueModel(SQSModel):
        val: str

    queue = async_queue_fixture
    queue.register_model(NoQueueModel)

    instance = NoQueueModel(val="random")
    with pytest.raises(exceptions.MessageNotInQueueError):
        await instance.delete_from_queue()