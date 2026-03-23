import socket
import struct
import time
import asyncio

from .proto.udp_payload_pb2 import *
from .util import resolveAddress

__all__ = [
    'MulticastReader',
    'MulticastWriter'
]

class _MulticastProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue):
        self._queue = queue
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        self._queue.put_nowait(data)

    def error_received(self, exc):
        pass

class MulticastReader:
    def __init__(self, multicastAddress: str, multicastPort: int, bindAddress: str = ''):
        self._address = multicastAddress
        self._port = multicastPort
        self._bind = resolveAddress(bindAddress)
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._transport = None
        self._protocol = None
        self._sock = None
        self._mreq = None

    async def join(self):
        loop = asyncio.get_running_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._bind, self._port))

        mreq = struct.pack("4sl", socket.inet_aton(self._address), socket.INADDR_ANY)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.setblocking(False)

        self._mreq = mreq
        self._sock = sock

        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _MulticastProtocol(self._queue),
            sock=sock
        )

    async def leave(self):
        if self._sock and self._mreq:
            try:
                self._sock.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, self._mreq)
            except OSError:
                pass

    def close(self):
        if self._transport:
            self._transport.close()
            self._transport = None

    async def readUdpPayload(self, timeout: float = 1.0) -> UdpPayload | None:
        try:
            data = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            if len(data) == 0:
                return None
            udpPayload = UdpPayload()
            udpPayload.ParseFromString(data)
            return udpPayload
        except asyncio.TimeoutError:
            raise TimeoutError()

class MulticastWriter:
    def __init__(self, multicastAddress: str, multicastPort: int, source_id: int, bindAddress: str = ''):
        self._address = multicastAddress
        self._port = multicastPort
        self._bind = resolveAddress(bindAddress)
        self._msg = UdpPayload()
        self._msg.source_id = source_id
        self._transport = None

    async def open(self):
        loop = asyncio.get_running_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self._bind, self._port))
        sock.setblocking(False)

        self._transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol,
            sock=sock
        )

    def sendPayload(self, payload: bytes, type: str, sequence: int) -> int:
        msg = self._msg
        msg.sequence = sequence
        msg.timestamp = time.time_ns()
        msg.type = type
        msg.payload = payload

        data = msg.SerializeToString()
        self._transport.sendto(data, (self._address, self._port))
        return len(data)

    def close(self):
        if self._transport:
            self._transport.close()
            self._transport = None
