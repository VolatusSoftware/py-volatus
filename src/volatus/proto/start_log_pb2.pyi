from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class StartLog(_message.Message):
    __slots__ = ("series", "timestamp", "started_by", "groups")
    SERIES_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    STARTED_BY_FIELD_NUMBER: _ClassVar[int]
    GROUPS_FIELD_NUMBER: _ClassVar[int]
    series: str
    timestamp: str
    started_by: str
    groups: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, series: _Optional[str] = ..., timestamp: _Optional[str] = ..., started_by: _Optional[str] = ..., groups: _Optional[_Iterable[str]] = ...) -> None: ...
