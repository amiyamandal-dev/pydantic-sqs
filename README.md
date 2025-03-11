Below is an example `README.md` that you could place at the root of your repository. It explains what this library is, how to install it, and how to use both the **asynchronous** and **synchronous** workflows with **pydantic_sqs**.

---

# pydantic-sqs

**pydantic-sqs** is a thin wrapper around AWS SQS that combines [Pydantic](https://docs.pydantic.dev/) models with the power of [aiobotocore](https://github.com/aio-libs/aiobotocore) (for async usage) and [boto3](https://github.com/boto/boto3) (for sync usage). It provides:

- **Pydantic**-based models (`SQSModel`) that can be serialized/deserialized automatically.
- An **async** queue wrapper (`SQSQueue`) for working with SQS in an event-driven or asynchronous setup.
- A **sync** queue wrapper (`SQSQueueSync`) for traditional synchronous usage.
- Batch send/delete operations.
- Validation and error handling (e.g., unknown or invalid messages).

---

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Creating Models](#creating-models)
4. [Registering Models to a Queue](#registering-models-to-a-queue)
5. [Sending and Receiving Messages (Async)](#sending-and-receiving-messages-async)
6. [Sending and Receiving Messages (Sync)](#sending-and-receiving-messages-sync)
7. [Error Handling](#error-handling)
8. [Advanced Features](#advanced-features)
9. [Testing with LocalStack](#testing-with-localstack)

---

## Installation

```bash
# If you have a pyproject.toml or requirements.txt, add pydantic_sqs
# If this is a local package, you may need to install it via pip -e .
pip install pydantic_sqs

# Also ensure you have aiobotocore and boto3 installed (often pydantic_sqs requires them):
pip install aiobotocore boto3 pydantic
```

---

## Quick Start

1. **Create one or more Pydantic models** by subclassing `SQSModel`.
2. **Create** either an `SQSQueue` (async) or `SQSQueueSync` (sync) object, pointing it to your SQS queue URL.
3. **Register** your models with that queue (so the library knows how to deserialize them).
4. **Send** and **receive** messages using the provided methods.

---

## Creating Models

All models must subclass `pydantic_sqs.SQSModel`. Example:

```python
from pydantic_sqs import SQSModel

class MyModel(SQSModel):
    foo: str
    bar: int
```

Each `SQSModel` automatically includes some special fields (like `message_id`, `receipt_handle`, etc.) and implements methods to send/receive from SQS.

---

## Registering Models to a Queue

Before you can send or receive a particular model, you have to tell the queue which models it supports, by calling `register_model()`.

For example, if you’re using the asynchronous `SQSQueue`:

```python
from pydantic_sqs import SQSQueue

# Suppose you already have your queue URL and region
queue_url = "https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue"
aws_region = "us-east-1"

# Create the async SQS queue
queue = SQSQueue(
    queue_url=queue_url,
    aws_region=aws_region,
    use_ssl=True
)

# Register your pydantic models
queue.register_model(MyModel)
```

For the **sync** queue, you’d do the same, but create an `SQSQueueSync` instance:

```python
from pydantic_sqs import SQSQueueSync

queue_sync = SQSQueueSync(
    queue_url=queue_url,
    aws_region=aws_region,
    use_ssl=True
)

queue_sync.register_model(MyModel)
```

Once registered, you can send/receive instances of `MyModel` directly.

---

## Sending and Receiving Messages (Async)

```python
import asyncio
from pydantic_sqs import SQSModel, SQSQueue

class MyModel(SQSModel):
    foo: str
    bar: int

async def main():
    # 1. Create the queue instance
    queue = SQSQueue(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        aws_region="us-east-1",
        use_ssl=True
    )
    
    # 2. Register your model
    queue.register_model(MyModel)

    # 3. Create a model instance
    model_instance = MyModel(foo="Hello", bar=123)

    # 4. Send it to SQS
    await model_instance.to_sqs()
    print("Sent message with ID:", model_instance.message_id)

    # 5. Receive messages back from SQS
    #    By default, this will block (long-poll) up to wait_time_seconds, or
    #    raise an error if the queue is empty (unless you set ignore_empty=True).
    received_models = await queue.from_sqs(
        max_messages=1,
        ignore_empty=False
    )
    for m in received_models:
        print("Got model from SQS:", m)
        # 6. Delete it from SQS when done (optional)
        await m.delete_from_queue()

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Sending and Receiving Messages (Sync)

```python
from pydantic_sqs import SQSModel, SQSQueueSync

class MyModel(SQSModel):
    foo: str
    bar: int

def main():
    # 1. Create the sync queue instance
    queue_sync = SQSQueueSync(
        queue_url="https://sqs.us-east-1.amazonaws.com/123456789012/MyQueue",
        aws_region="us-east-1",
        use_ssl=True
    )

    # 2. Register your model
    queue_sync.register_model(MyModel)

    # 3. Create a model instance
    model_instance = MyModel(foo="Hello from sync", bar=42)

    # 4. Send it to SQS synchronously
    model_instance.to_sqs_sync()
    print("Sent message with ID:", model_instance.message_id)

    # 5. Receive messages back from SQS
    received_models = queue_sync.from_sqs_sync(
        max_messages=1, 
        ignore_empty=True
    )
    for m in received_models:
        print("Got model from SQS:", m)
        # 6. Delete it from SQS
        m.delete_from_queue_sync()

if __name__ == "__main__":
    main()
```

---

## Error Handling

The library raises custom exceptions in `pydantic_sqs.exceptions`, such as:

- **`NotRegisteredError`**: Raised if you try to send/receive a model that has not been registered with a queue.
- **`MsgNotFoundError`**: Raised if you try to read from an empty queue (unless `ignore_empty=True` is used).
- **`InvalidMessageInQueueError`**: Raised if the queue contains messages the library can’t deserialize or validate.
- **`MessageNotInQueueError`**: Raised if you attempt to delete a message that has no `receipt_handle`, or has already been deleted.
- **`ModelAlreadyRegisteredError`**: Raised if you try to register a model class multiple times on the same queue.

You can either handle these individually or catch them at a higher level as needed.

---

## Advanced Features

1. **Batch Sends and Batch Deletes**  
   You can send up to 10 messages at once:

   ```python
   # Async
   model_list = [MyModel(foo=f"Hello {i}", bar=i) for i in range(5)]
   await queue.send_messages_batch(model_list)
   ```

   ```python
   # Sync
   model_list = [MyModel(foo=f"Hello {i}", bar=i) for i in range(5)]
   queue_sync.send_messages_batch(model_list)
   ```

   Same pattern applies for `delete_messages_batch` with up to 10 messages at once.

2. **Custom Serialization**  
   By default, this library uses JSON for serialization. You can opt for `msgpack` by setting:
   ```python
   queue = SQSQueue(queue_url=..., serializer="msgpack")
   ```
   or
   ```python
   queue_sync = SQSQueueSync(queue_url=..., serializer="msgpack")
   ```
   The library will then automatically serialize and deserialize using MessagePack instead of JSON.

3. **Visibility Timeout / Wait Time**  
   You can override queue parameters such as `visibility_timeout`, `max_messages`, `wait_time_seconds` directly via the queue constructor, or pass them in methods like `from_sqs`.

---

## Testing with LocalStack

The repository’s test suite demonstrates how to run SQS-based tests against [LocalStack](https://docs.localstack.cloud/). Essentially:

1. **Start LocalStack** (e.g., via Docker).
2. **Create** a queue in LocalStack.
3. **Point** your `SQSQueue` or `SQSQueueSync` at `endpoint_url="http://localhost:4566"` and `use_ssl=False`.
4. **Run** your tests or script.

Example fixture (sync version):

```python
import pytest
import boto3
from pydantic_sqs import SQSQueueSync

@pytest.fixture
def sync_queue_fixture():
    endpoint_url = "http://localhost:4566"
    aws_region = "us-east-1"
    queue_name = "my-localstack-queue"

    # Create the queue in LocalStack
    sqs_client = boto3.client(
        "sqs",
        region_name=aws_region,
        use_ssl=False,
        endpoint_url=endpoint_url,
    )
    create_resp = sqs_client.create_queue(QueueName=queue_name)
    queue_url = create_resp["QueueUrl"]

    # Create SQSQueueSync pointing to that queue
    queue = SQSQueueSync(
        queue_url=queue_url,
        aws_region=aws_region,
        endpoint_url=endpoint_url,
        use_ssl=False,
    )

    yield queue

    # Clean up after test
    sqs_client.delete_queue(QueueUrl=queue_url)
```

From there, you can write tests that inject `sync_queue_fixture`, register models, send messages, receive them, etc.

---

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests to improve features, documentation, or test coverage.

---

## License

Add your chosen license here (e.g., MIT, Apache 2.0, etc.). If it’s not yet decided, you can leave this section out or include something like:

```
This project is licensed under the MIT License - see the LICENSE.md file for details.
```

---

**Happy messaging with pydantic-sqs!**