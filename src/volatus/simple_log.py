import struct
import time
import asyncio
import copy
import functools
from datetime import datetime, timezone
from collections import deque
from collections.abc import Iterable
from pathlib import Path
import numpy as np
from dataclasses import dataclass
from enum import Enum
import aiofiles

from .config import GroupConfig

# Format of file:
# SLOG
# <section type> 1 byte
# <section length> 4 bytes (excluded for section type of Data)
# <encoded section data> section type dependent

# Sections Types:

# MetaField
# <name> String prepended with 4 bytes of length
# <val type> Byte of ValType value
# <meta fields>

class ValType(Enum):
    UNK = 0
    I64 = 1
    U64 = 2
    F32 = 3
    F64 = 4
    STR = 5
    BOOL = 6

    def encode(self) -> bytes:
        return struct.pack('b', self.value)

class Section(Enum):
    UNK = 0
    Data = 1
    MetaList = 2
    MetaField = 3
    ChanList = 4
    Channel = 5

    def encode(self) -> bytes:
        return struct.pack('b', self.value)

class MetaField:
    type: ValType = ValType.UNK
    name: str
    val: bytes

    _token = object()

    def __init__(self, token, type: ValType, name: str, val: bytes):
        if not token == self._token:
            raise RuntimeError("Cannot create MetaFields directly, please use a from_* static method.")

        self.type = type
        self.name = name
        self.val = val

    @staticmethod
    def from_I64(name: str, val: int):
        return MetaField(MetaField._token, ValType.I64, name, struct.pack("<q", val))

    @staticmethod
    def from_U64(name: str, val: int):
        return MetaField(MetaField._token, ValType.U64, name, struct.pack("<Q", val))

    @staticmethod
    def from_F32(name: str, val: float):
        return MetaField(MetaField._token, ValType.F32, name, struct.pack("<f", val))

    @staticmethod
    def from_F64(name: str, val: float):
        return MetaField(MetaField._token, ValType.F64, name, struct.pack("<d", val))

    @staticmethod
    def from_STR(name: str, val: str):
        data = val.encode()
        l = len(data)
        return MetaField(MetaField._token, ValType.STR, name, struct.pack(f"<I{l}s", l, data))

    @staticmethod
    def from_BOOL(name: str, val: bool):
        return MetaField(MetaField._token, ValType.BOOL, name, struct.pack("b", 1 if val else 0))

    def as_int(self) -> int:
        if not self.type in [ValType.I64, ValType.U64, ValType.BOOL]:
            raise TypeError("Instance not created as a numeric type.")
        
        return int.from_bytes(self.val)

    def as_str(self) -> str:
        if not self.type == ValType.STR:
            raise TypeError("Instance not created as a string type.")

        return self.val[4:].decode() #ignore 4 bytes of prepended length

    def as_bool(self) -> bool:
        if not self.type == ValType.BOOL:
            raise TypeError("Instance not created as a bool type.")

        return self.val != 0

    def as_float(self) -> float:
        if not self.type in [ValType.F32, ValType.F64]:
            raise TypeError("Instance not created as a float type.")

        if self.type == ValType.F32:
            return struct.unpack("<f", self.val)

        return struct.unpack("<d", self.val)

type MetaList = list[MetaField]
type MetaDict = dict[str, MetaField]

class ChannelInfo:
    def __init__(self, name: str, type: ValType, meta: MetaList = []):
        if type == ValType.STR:
            raise TypeError("Channels do not support string type values.")
        
        self.type = type
        self._meta = {field.name: field for field in meta}

        self.set_name(name)

    def set_meta(self, field: MetaField):
        self._meta[field.name] = field

    def get_meta(self, name:str) -> MetaField:
        return self._meta[name]

    def get_name(self) -> str:
        return self._meta["name"].as_str()

def _chan_fmt(type: ValType, str_len: int = -1) -> str:
    match type:
        case ValType.I64:
            return 'q'
        case ValType.U64:
            return 'Q'
        case ValType.F32:
            return 'f'
        case ValType.F64:
            return 'd'
        case ValType.BOOL:
            return 'b'
        case ValType.STR:
            raise ValueError("Strings are not supported for encoded data.")

class FileInfo:
    def __init__(self, meta: MetaList = [], channel_info: list[ChannelInfo] = []):
        self._meta = {field.name: field for field in meta}
        self._chans = {channel.name: channel for channel in channel_info}

    def set_meta(self, meta: MetaField):
        self._meta[meta.name] = meta

    def get_meta(self, name: str) -> MetaField:
        return self._meta[name]

    def set_channel(self, chan: ChannelInfo):
        self._chans[chan.name] = chan

    def get_channel(self, name: str) -> ChannelInfo:
        return self._chans[name]

    def entry_format(self) -> str:
        return ''.join(['<'] + [_chan_fmt(chan.type) for _, chan in self._chans.items()])

class SimpleLog:
    def __init__(self, info: FileInfo = FileInfo()):
        self._started = False
        self._info = info
        self._path: Path = None
        self._format: str = ""
        self._file = None

    @staticmethod
    def for_group(group_cfg: GroupConfig) -> 'SimpleLog':
        chans: list[ChannelInfo] = []

        for chan_name, chan_cfg in group_cfg.channels.items():
            chan = ChannelInfo(chan_name, ValType.F64)
            unit = chan_cfg.lookupChildByName("Unit")
            if unit:
                chan.set_meta(MetaField.from_STR("unit", unit.value()))

            resource = chan_cfg.lookupChildByName("Resource")
            if resource:
                chan.set_meta(MetaField.from_STR("resource", resource.value()))

            chans.append(chan)

        metas: MetaList = []
        
        metas.append(MetaField.from_STR("cluster", group_cfg.clusterName))
        metas.append(MetaField.from_STR("node", group_cfg.nodeName))
        metas.append(MetaField.from_STR("task", group_cfg.taskName))
        metas.append(MetaField.from_STR("group", group_cfg.name))

        file_info = FileInfo(metas, chans)

        return SimpleLog(file_info)

    def _check_not_started(self):
        if self._started:
            raise RuntimeError("Cannot modify info once logging has started.")

    def _check_started(self):
        if not self._started:
            raise RuntimeError("Log must be started.")

    def set_info(self, info: FileInfo):
        self._check_not_started()
        
        self._info = info
    
    def get_info(self) -> FileInfo:
        return self._info

    def set_meta(self, meta: MetaField):
        self._check_not_started()

        self._info.set_meta(meta)

    def get_meta(self, name: str) -> MetaField:
        return self._info.get_meta(name)

    def set_chan_info(self, chan: ChannelInfo):
        self._check_not_started()

        self._info.set_channel(chan)

    def get_chan_info(self, name: str) -> ChannelInfo:
        return self._info.get_channel(name)

    def set_chan_meta(self, chan_name: str, meta: MetaField):
        self._check_not_started()

        self._info.get_channel(chan_name).set_meta(meta)

    def get_chan_meta(self, chan_name: str, meta_name: str) -> MetaField:
        return self._info.get_channel(chan_name).get_meta(meta_name)

    def _encode_str(self, val: str) -> bytes:
        l = len(str)
        return struct.pack(f"<I{l}s", l, val)

    def _encode_length(self, l: int) -> bytes:
        return struct.pack("<l", l)

    def _encode_section(self, type: Section, section: bytes) -> bytes:
        return type.encode() + self._encode_length(section) + section

    def _encode_meta(self, meta: MetaField) -> bytes:
        buf += self._encode_str(meta.name) # 4 byte string length + string bytes
        buf += meta.type.encode()          # byte of meta type
        buf += meta.val                    # value is stored encoded already

        return self._encode_section(Section.MetaField, buf)

    def _encode_meta_dict(self, metas: MetaDict, skip_names: list[str] = []) -> bytes:
        buf = b""
        for name, meta in metas.items():
            if not name in skip_names:
                buf += self._encode_meta(meta)

        return self._encode_section(Section.MetaList, buf)

    def _encode_chan(self, chan: ChannelInfo) -> bytes:
        buf += self._encode_str(chan.get_name())
        buf = chan.type.encode()
        buf += self._encode_meta_dict(chan._meta, ["name"]) # skips name field, putting that up front

        return self._encode_section(Section.Channel, buf)

    def _encode_chan_dict(self, chans: dict[str, ChannelInfo]) -> bytes:
        buf = b""

        for _, chan in chans.items():
            buf += self._encode_chan(chan)

        return self._encode_section(Section.ChanList, buf)

    async def _write_info(self):
        self._file.write(b"SLOG\n") # File type identifier

        await self._file.write(self._encode_meta_dict(self._info._meta))
        await self._file.write(self._encode_chan_dict(self._info._chans))

        # start data segment which runs to end of file
        await self._file.write(Section.Data.encode() + b"\n")

    async def start(self, path: Path):
        self._check_not_started()

        self._file = await aiofiles.open(path, "wb")
        self._path = path

        self.set_meta(MetaField.from_STR("started", datetime.now(timezone.utc).replace(microsecond=0).isoformat()))

        await self._write_info()
        self._format = self._info.entry_format()

        self._started = True

    def _format_entry(self, timestamp: int, data: np.ndarray) -> bytes:
        if len(data.shape) != 1:
            raise ValueError("Array must be single dimension.")

        return struct.pack('<Q', timestamp) + struct.pack(self._format, data) + b"\n"

    async def write_entry(self, timestamp: int, data: Iterable[float]):
        self._check_started()

        await self._file.write(self._format_entry(timestamp, data))

    async def stop(self):
        self._check_started()

        self._started = False

        await self._file.close()
        self._file = None
        # Leave _path intact for followup queries until next log started