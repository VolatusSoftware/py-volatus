from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class StopLog(_message.Message):
    __slots__ = ("reason", "groups")
    REASON_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    reason: str
    groups: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, reason: _Optional[str] = ..., groups: _Optional[_Iterable[str]] = ...) -> None: ...
