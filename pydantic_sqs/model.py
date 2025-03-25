from typing import Any, Dict, Optional
from loguru import logger
from pydantic import field_validator, model_validator

from pydantic_sqs import exceptions
from pydantic_sqs.abstract import _AbstractModel


class SQSModel(_AbstractModel):
    """
    A SQSModel is a pydantic model that can be sent to and from SQS.
    """
    message_id: Optional[str] = None
    receipt_handle: Optional[str] = None
    attributes: Optional[Dict[str, str]] = None
    deleted: bool = False

    def get(self, key, default=None):
        return getattr(self, key, default)

    @field_validator("attributes", mode="before")
    def check_attributes_dict(cls, v):
        if v is None:
            return None
        if not isinstance(v, dict):
            logger.error("Attributes provided are not a dict.")
            raise ValueError("attributes must be a dict")
        for key, val in v.items():
            if not isinstance(key, str) or not isinstance(val, str):
                logger.error("Attributes keys/values are not strings.")
                raise ValueError("attributes keys/values must be strings")
        return v

    @model_validator(mode="after")
    def ensure_message_id_and_handle(self):
        if self.message_id and not self.receipt_handle:
            logger.error("message_id provided without a corresponding receipt_handle.")
            raise ValueError("message_id requires receipt_handle")
        return self

    def __send_kwargs(self, queue_url: str, wait_time_in_seconds: int = None) -> Dict[str, Any]:
        send_kwargs = {}
        if wait_time_in_seconds is not None:
            wait_time_in_seconds = max(0, min(wait_time_in_seconds, 900))
            send_kwargs["DelaySeconds"] = wait_time_in_seconds

        body_data = {
            "model": self.__class__.__qualname__.lower(),
            "message": self.model_dump(exclude_unset=True),
        }
        queue = self.__get_queue()
        serialized_body = queue._serialize(body_data)
        send_kwargs["QueueUrl"] = queue_url
        send_kwargs["MessageBody"] = serialized_body
        return send_kwargs

    @classmethod
    def __get_queue(cls) -> _AbstractModel:
        try:
            return cls._queue
        except AttributeError:
            logger.error(f"{cls.__qualname__} not registered to a queue.")
            raise exceptions.NotRegisteredError(
                f"{cls.__qualname__} not registered to a queue"
            ) from None

    @classmethod
    async def from_sqs(cls, max_messages=None, visibility_timeout=None,
                       wait_time_seconds=None, ignore_empty=True, parallel_processing: bool = False):
        queue = cls.__get_queue()
        results = await queue.from_sqs(
            max_messages=max_messages,
            visibility_timeout=visibility_timeout,
            wait_time_seconds=wait_time_seconds,
            ignore_empty=ignore_empty,
            ignore_unknown=True,
            parallel_processing=parallel_processing
        )
        logger.info(f"Retrieved {len(results)} messages from SQS for model {cls.__qualname__}")
        return [res for res in results if isinstance(res, cls)]

    async def to_sqs(self, wait_time_in_seconds=None):
        queue = self.__get_queue()
        send_kwargs = self.__send_kwargs(queue.queue_url, wait_time_in_seconds)
        logger.debug(f"Sending message to SQS with parameters: {send_kwargs}")
        async with queue.session.create_client("sqs", **queue.client_kwargs) as client:
            response = await client.send_message(**send_kwargs)
        self.message_id = response["MessageId"]
        logger.info(f"Message sent to SQS. MessageId: {self.message_id}")

    async def delete_from_queue(self):
        if self.receipt_handle is None:
            logger.error("Attempted to delete a message without receipt_handle.")
            raise exceptions.MessageNotInQueueError("No receipt_handle to delete.")
        if self.deleted:
            logger.error("Attempted to delete a message that is already marked as deleted.")
            raise exceptions.MessageNotInQueueError("Already deleted.")
        queue = self.__get_queue()
        logger.debug(f"Deleting message from queue {queue.queue_url} with receipt_handle: {self.receipt_handle}")
        async with queue.session.create_client("sqs", **queue.client_kwargs) as client:
            await client.delete_message(
                QueueUrl=queue.queue_url, ReceiptHandle=self.receipt_handle
            )
        self.deleted = True
        logger.info("Message successfully deleted from SQS.")

    @classmethod
    def from_sqs_sync(cls, max_messages=None, visibility_timeout=None,
                      wait_time_seconds=None, ignore_empty=True):
        queue = cls.__get_queue()
        results = queue.from_sqs_sync(
            max_messages=max_messages,
            visibility_timeout=visibility_timeout,
            wait_time_seconds=wait_time_seconds,
            ignore_empty=ignore_empty,
            ignore_unknown=True
        )
        logger.info(f"Synchronously retrieved {len(results)} messages for model {cls.__qualname__}")
        return [res for res in results if isinstance(res, cls)]

    def to_sqs_sync(self, wait_time_in_seconds=None):
        queue = self.__get_queue()
        send_kwargs = self.__send_kwargs(queue.queue_url, wait_time_in_seconds)
        logger.debug(f"Synchronously sending message to SQS with parameters: {send_kwargs}")
        response = queue.client.send_message(**send_kwargs)
        self.message_id = response["MessageId"]
        logger.info(f"Message synchronously sent to SQS. MessageId: {self.message_id}")

    def delete_from_queue_sync(self):
        if self.receipt_handle is None:
            logger.error("Attempted synchronous deletion without receipt_handle.")
            raise exceptions.MessageNotInQueueError("No receipt_handle to delete.")
        if self.deleted:
            logger.error("Attempted synchronous deletion of an already deleted message.")
            raise exceptions.MessageNotInQueueError("Already deleted.")
        queue = self.__get_queue()
        logger.debug(f"Synchronously deleting message from queue {queue.queue_url} with receipt_handle: {self.receipt_handle}")
        queue.client.delete_message(
            QueueUrl=queue.queue_url, ReceiptHandle=self.receipt_handle
        )
        self.deleted = True
        logger.info("Message successfully deleted synchronously from SQS.")
