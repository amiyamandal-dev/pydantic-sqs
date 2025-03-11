import pytest
import asyncio
from pydantic_sqs.queue import SQSQueue, SQSQueueSync
from pydantic_sqs.model import SQSModel
from pydantic_sqs import exceptions


class DummyModel(SQSModel):
    val: str


@pytest.mark.asyncio
async def test_async_queue_properties(async_queue_fixture):
    """
    Ensure the queue fixture is set up correctly for async usage.
    """
    queue = async_queue_fixture
    assert isinstance(queue, SQSQueue)
    assert queue.aws_region == "us-east-1"
    assert queue.serializer == "json"


def test_sync_queue_properties(sync_queue_fixture):
    """
    Ensure the queue fixture is set up correctly for sync usage.
    """
    queue = sync_queue_fixture
    assert isinstance(queue, SQSQueueSync)
    assert queue.aws_region == "us-east-1"
    assert queue.serializer == "json"


def test_model_already_registered(sync_queue_fixture):
    """
    Trying to register the same model again should raise ModelAlreadyRegisteredError.
    """
    queue = sync_queue_fixture
    queue.register_model(DummyModel)
    with pytest.raises(exceptions.ModelAlreadyRegisteredError):
        queue.register_model(DummyModel)


@pytest.mark.asyncio
async def test_unknown_message(async_queue_fixture):
    """
    If a message arrives with a 'model' key that isn't in the queue's registry,
    we raise InvalidMessageInQueueError unless ignore_unknown=True.
    """
    queue = async_queue_fixture

    class GoodModel(SQSModel):
        foo: str
    queue.register_model(GoodModel)

    class BadModel(SQSModel):
        bar: int

    queue.register_model(BadModel)
    bad_instance = BadModel(bar=123)
    await bad_instance.to_sqs()
    del queue.models["badmodel"]  # forcibly remove registration

    with pytest.raises(exceptions.InvalidMessageInQueueError):
        await queue.from_sqs(ignore_unknown=False)

    msgs = await queue.from_sqs(ignore_unknown=True)
    assert len(msgs) == 0


@pytest.mark.asyncio
async def test_parallel_processing(async_queue_fixture):
    """
    Demonstrate parallel message processing with async + SQSQueue.
    """
    queue = async_queue_fixture
    queue.register_model(DummyModel)

    items = [DummyModel(val=f"parallel-{i}") for i in range(5)]
    await queue.send_messages_batch(items)

    sem = asyncio.Semaphore(2)  # concurrency=2

    async def process_message(m: DummyModel):
        async with sem:
            await asyncio.sleep(0.1)  # simulate I/O
            await m.delete_from_queue()

    while True:
        msgs = await queue.from_sqs(ignore_empty=True, max_messages=5)
        if not msgs:
            break
        tasks = [asyncio.create_task(process_message(msg)) for msg in msgs]
        await asyncio.gather(*tasks)

    leftover = await queue.from_sqs(ignore_empty=True)
    assert len(leftover) == 0