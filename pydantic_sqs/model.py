import json
from typing import Any, Dict, List, Optional

from pydantic import field_validator, model_validator, Field
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

    @field_validator("attributes", mode="before")
    def check_attributes_dict(cls, v):
        """
        Ensure attributes is either None or a dict[str, str].
        """
        if v is None:
            return None
        if not isinstance(v, dict):
            raise ValueError("attributes must be a dict")
        for key, val in v.items():
            if not isinstance(key, str) or not isinstance(val, str):
                raise ValueError("attributes keys/values must be strings")
        return v

    @model_validator(mode='after')
    def ensure_message_id_and_handle(cls, values):
        """
        Ensure message_id and receipt_handle are consistent.
        """
        msg_id = values.get("message_id")
        r_handle = values.get("receipt_handle")
        if msg_id and not r_handle:
            raise ValueError("message_id requires receipt_handle")
        return values

    def __send_kwargs(self, queue_url: str, wait_time_in_seconds: int = None) -> Dict[str, Any]:
        """
        Create the send kwargs for either async or sync usage.
        """
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
        """
        Get the queue that this model is registered to.
        """
        try:
            return cls._queue
        except AttributeError:
            raise exceptions.NotRegisteredError(
                f"{cls.__qualname__} not registered to a queue"
            ) from None

    @classmethod
    async def from_sqs(cls, max_messages=None, visibility_timeout=None,
                       wait_time_seconds=None, ignore_empty=True):
        """
        Asynchronously gets this model from the queue.
        """
        queue = cls.__get_queue()
        results = await queue.from_sqs(
            max_messages=max_messages,
            visibility_timeout=visibility_timeout,
            wait_time_seconds=wait_time_seconds,
            ignore_empty=ignore_empty,
            ignore_unknown=True
        )
        return [res for res in results if isinstance(res, cls)]

    async def to_sqs(self, wait_time_in_seconds=None):
        """
        Asynchronously send this object to SQS.
        """
        queue = self.__get_queue()
        send_kwargs = self.__send_kwargs(queue.queue_url, wait_time_in_seconds)
        async with queue.session.create_client("sqs", **queue.client_kwargs) as client:
            response = await client.send_message(**send_kwargs)
        self.message_id = response["MessageId"]

    async def delete_from_queue(self):
        """
        Asynchronously delete this object from SQS.
        """
        if self.receipt_handle is None:
            raise exceptions.MessageNotInQueueError("No receipt_handle to delete.")
        if self.deleted:
            raise exceptions.MessageNotInQueueError("Already deleted.")

        queue = self.__get_queue()
        async with queue.session.create_client("sqs", **queue.client_kwargs) as client:
            await client.delete_message(
                QueueUrl=queue.queue_url, ReceiptHandle=self.receipt_handle
            )
        self.deleted = True

    @classmethod
    def from_sqs_sync(cls, max_messages=None, visibility_timeout=None,
                      wait_time_seconds=None, ignore_empty=True):
        """
        Synchronously gets this model from the queue.
        """
        queue = cls.__get_queue()
        results = queue.from_sqs_sync(
            max_messages=max_messages,
            visibility_timeout=visibility_timeout,
            wait_time_seconds=wait_time_seconds,
            ignore_empty=ignore_empty,
            ignore_unknown=True
        )
        return [res for res in results if isinstance(res, cls)]

    def to_sqs_sync(self, wait_time_in_seconds=None):
        """
        Synchronously send this object to SQS.
        """
        queue = self.__get_queue()
        send_kwargs = self.__send_kwargs(queue.queue_url, wait_time_in_seconds)
        response = queue.client.send_message(**send_kwargs)
        self.message_id = response["MessageId"]

    def delete_from_queue_sync(self):
        """
        Synchronously delete this object from SQS.
        """
        if self.receipt_handle is None:
            raise exceptions.MessageNotInQueueError("No receipt_handle to delete.")
        if self.deleted:
            raise exceptions.MessageNotInQueueError("Already deleted.")

        queue = self.__get_queue()
        queue.client.delete_message(
            QueueUrl=queue.queue_url, ReceiptHandle=self.receipt_handle
        )
        self.deleted = True