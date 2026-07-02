from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TcpClientInfo(_message.Message):
    __slots__ = ("node_id", "node_name", "app_version", "config_version", "config_hash", "node_alias", "address")
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_NAME_FIELD_NUMBER: _ClassVar[int]
    APP_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONFIG_VERSION_FIELD_NUMBER: _ClassVar[int]
    CONFIG_HASH_FIELD_NUMBER: _ClassVar[int]
    NODE_ALIAS_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    node_id: int
    node_name: str
    app_version: str
    config_version: str
    config_hash: str
    node_alias: str
    address: str
    def __init__(self, node_id: _Optional[int] = ..., node_name: _Optional[str] = ..., app_version: _Optional[str] = ..., config_version: _Optional[str] = ..., config_hash: _Optional[str] = ..., node_alias: _Optional[str] = ..., address: _Optional[str] = ...) -> None: ...

class TcpClientList(_message.Message):
    __slots__ = ("clients",)
    CLIENTS_FIELD_NUMBER: _ClassVar[int]
    clients: _containers.RepeatedCompositeFieldContainer[TcpClientInfo]
    def __init__(self, clients: _Optional[_Iterable[_Union[TcpClientInfo, _Mapping]]] = ...) -> None: ...
