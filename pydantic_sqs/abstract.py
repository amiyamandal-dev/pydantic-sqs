"""Module containing the main base classes"""
from pydantic import BaseModel


class _AbstractQueue(BaseModel):
    """
    An abstract class of a queue.
    """
    queue_url: str
    aws_region: str = "us-east-1"
    serializer: str = "json"  # Default to JSON; can be 'msgpack'

    model_config = {
        "arbitrary_types_allowed": True,
        "from_attributes": True,
    }


class _AbstractModel(BaseModel):
    """
    An abstract class to help with typings for Model class.
    """
    _queue: _AbstractQueue

    model_config = {
        "arbitrary_types_allowed": True,
        "from_attributes": True,
    }
