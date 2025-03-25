"""Module containing the queue classes."""
import base64
import binascii
import json
from typing import Any, Dict, List, Optional

import boto3
import msgpack
from pydantic import AnyUrl, Field, ValidationError

from pydantic_sqs import exceptions
from pydantic_sqs.abstract import _AbstractQueue
from pydantic_sqs.model import SQSModel


class BaseSQSQueue(_AbstractQueue):
    """
    Base class containing shared logic for both sync and async queue implementations.
    """

    models: Dict[str, type(SQSModel)] = {}
    serializer: str = "json"  # JSON or 'msgpack'

    # Fine-tuned msgpack settings
    _msgpack_pack_params = {"use_bin_type": True, "strict_types": True}
    _msgpack_unpack_params = {"strict_map_key": False, "raw": False, "unicode_errors": "replace"}

    def register_model(self, model_class: SQSModel):
        """
        Add a model to this queue.
        """
        model_name = model_class.__qualname__.lower()
        if model_name in self.models:
            raise exceptions.ModelAlreadyRegisteredError(
                f"{model_class.__qualname__} is already registered "
                f"to {model_class._queue.queue_url}"
            )
        model_class._queue = self
        self.models[model_name] = model_class

    def _serialize(self, data: Dict[str, Any]) -> str:
        """
        Serialize a dict to either JSON or Base64-encoded MessagePack.
        """
        if self.serializer == "json":
            return json.dumps(data)
        elif self.serializer == "msgpack":
            packed = msgpack.packb(data, **self._msgpack_pack_params)
            return base64.b64encode(packed).decode("utf-8")
        else:
            raise ValueError(f"Unsupported serializer: {self.serializer}")

    def _deserialize(self, data: str) -> Dict[str, Any]:
        if self.serializer == "msgpack":
            try:
                raw = base64.b64decode(data, validate=True)
            except (binascii.Error, ValueError) as e:
                raise exceptions.InvalidMessageInQueueError(
                    f"Invalid Base64 data in message: {str(e)}"
                ) from e
            try:
                return msgpack.unpackb(raw, **self._msgpack_unpack_params)
            except (msgpack.UnpackException, UnicodeDecodeError) as e:
                raise exceptions.InvalidMessageInQueueError(
                    f"Deserialization failed: {str(e)}"
                ) from e
        elif self.serializer == "json":
            return json.loads(data)
        else:
            raise ValueError(f"Unsupported serializer: {self.serializer}")

    def _message_to_object(
            self, message: Dict[str, Any],
            message_id: str,
            receipt_handle: str,
            attributes: Dict[str, str],
    ) -> SQSModel:
        """
        Convert a raw dictionary to the corresponding pydantic model instance.
        """
        try:
            model = self.models[message["model"]]
        except KeyError:
            raise exceptions.InvalidMessageInQueueError(
                f"No model registered for model type {message.get('model')} from {message_id}"
            )

        try:
            return model(
                message_id=message_id,
                receipt_handle=receipt_handle,
                attributes=attributes,
                **message["message"],
            )
        except ValidationError as exc:
            raise exceptions.InvalidMessageInQueueError(
                f"Invalid message {message_id} from queue {self.queue_url}"
            ) from exc



class SQSQueueSync(BaseSQSQueue):
    """
    A synchronous SQS queue using `boto3`.
    """

    client: Any = None
    endpoint_url: AnyUrl = None
    use_ssl: bool = True

    visibility_timeout: Optional[int] = None
    wait_time_seconds: Optional[int] = Field(default=None, ge=0, le=20)
    max_messages: int = Field(default=10, gt=0, le=10)

    def __init__(
            self,
            queue_url: str,
            aws_region: str = "us-east-1",
            client: Any = None,
            endpoint_url: AnyUrl = None,
            use_ssl: bool = True,
            serializer: str = "json",
            visibility_timeout: int = None,
            wait_time_seconds: int = None,
            max_messages: int = 10,
            **kwargs: Any,
    ):
        super().__init__(queue_url=queue_url, aws_region=aws_region, serializer=serializer)
        self.endpoint_url = endpoint_url
        self.use_ssl = use_ssl
        self.visibility_timeout = visibility_timeout
        self.wait_time_seconds = wait_time_seconds
        self.max_messages = max_messages

        if client is not None:
            self.client = client
        else:
            self.client = boto3.client(
                "sqs",
                region_name=self.aws_region,
                use_ssl=self.use_ssl,
                endpoint_url=self.endpoint_url,
            )

    def __recv_kwargs(
            self,
            max_messages: Optional[int],
            visibility_timeout: Optional[int],
            wait_time_seconds: Optional[int],
    ) -> Dict[str, Any]:
        kw = {
            "QueueUrl": self.queue_url,
            "MaxNumberOfMessages": max_messages if max_messages is not None else self.max_messages,
        }

        vt = visibility_timeout if visibility_timeout is not None else self.visibility_timeout
        if vt is not None:
            kw["VisibilityTimeout"] = vt

        wt = wait_time_seconds if wait_time_seconds is not None else self.wait_time_seconds
        if wt is not None:
            kw["WaitTimeSeconds"] = min(wt, 20)
        return kw

    def from_sqs_sync(
            self,
            max_messages=None,
            visibility_timeout=None,
            wait_time_seconds=None,
            ignore_empty=False,
            ignore_unknown=False,
    ) -> List[SQSModel]:
        recv_kwargs = self.__recv_kwargs(max_messages, visibility_timeout, wait_time_seconds)
        resp = self.client.receive_message(**recv_kwargs)
        messages = resp.get("Messages", [])

        if not messages and not ignore_empty:
            raise exceptions.MsgNotFoundError(f"{self.queue_url} is empty")

        results = []
        for msg in messages:
            raw_body = msg.get("Body", "")
            try:
                body_dict = self._deserialize(raw_body)
                results.append(
                    self._message_to_object(
                        message=body_dict,
                        message_id=msg["MessageId"],
                        receipt_handle=msg["ReceiptHandle"],
                        attributes=msg.get("Attributes", None),
                    )
                )
            except (json.JSONDecodeError, msgpack.ExtraData, msgpack.FormatError):
                if not ignore_unknown:
                    raise exceptions.InvalidMessageInQueueError(
                        f"Invalid body in message {msg['MessageId']}"
                    )
        return results
