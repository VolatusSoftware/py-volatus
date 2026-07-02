import socket
import struct
import time
import asyncio

from ..proto.udp_payload_pb2 import *
from .util import resolveAddress

__all__ = ["UdpReader", "UdpWriter"]


class _UdpProtocol(asyncio.DatagramProtocol):
    def __init__(
        self, queue: asyncio.Queue, multicast: bool, address: str = "", port: int = 0
    ):
        self._queue = queue
        self._multicast = multicast
        self._address = address
        self._port = port
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

        if not self._multicast:
            subMsg = UdpPayload()
            subMsg.type = "v:TelemClient"
            data = subMsg.SerializeToString()
            transport.sendto(data, (self._address, self._port))

    def datagram_received(self, data, addr):
        self._queue.put_nowait(data)

    def error_received(self, exc):
        pass

    def sendHeartbeat(self):
        if not self._multicast and self.transport is not None:
            subMsg = UdpPayload()
            subMsg.type = "v:TelemClient"
            data = subMsg.SerializeToString()
            self.transport.sendto(data, (self._address, self._port))


class UdpReader:
    def __init__(self, address: str, port: int, bindAddress: str = ""):
        self._address = address
        self._port = port
        self._bind = resolveAddress(bindAddress)
        self._queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._transport = None
        self._protocol = None
        self._sock = None
        self._mreq = None
        self._lastHB = time.time()
        self._multicast = False
        self._bindPort = 0

        first = int(address.split(".")[0])
        if first >= 224 and first < 240:
            self._multicast = True

        if self._multicast:
            self._bindPort = port

    async def join(self):
        loop = asyncio.get_running_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        if self._multicast:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind((self._bind, self._bindPort))

        if self._multicast:
            mreq = struct.pack(
                "4sl", socket.inet_aton(self._address), socket.INADDR_ANY
            )
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            self._mreq = mreq

        sock.setblocking(False)

        self._sock = sock

        self._transport, self._protocol = await loop.create_datagram_endpoint(
            lambda: _UdpProtocol(
                self._queue, self._multicast, self._address, self._port
            ),
            sock=sock,
        )

    async def leave(self):
        if self._multicast and self._sock and self._mreq:
            try:
                self._sock.setsockopt(
                    socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, self._mreq
                )
            except OSError:
                pass

    def close(self):
        if self._transport:
            self._transport.close()
            self._transport = None

    async def readUdpPayload(self, timeout: float = 1.0) -> UdpPayload | None:  
        if not self._multicast and self._protocol is not None:
            now = time.time()
            if now - self._lastHB > 1:
                self._protocol.sendHeartbeat()
                self._lastHB = now

        try:
            data = await asyncio.wait_for(self._queue.get(), timeout=timeout)
            if len(data) == 0:
                return None
            udpPayload = UdpPayload()
            udpPayload.ParseFromString(data)
            return udpPayload
        except asyncio.TimeoutError:
            raise TimeoutError()


class UdpWriter:
    def __init__(self, address: str, port: int, source_id: int, bindAddress: str = ""):
        self._address = address
        self._port = port
        self._bind = resolveAddress(bindAddress)
        self._msg = UdpPayload()
        self._msg.source_id = source_id
        self._transport = None
        self._multicast = False
        self._bindPort = 0

        first = int(address.split(".")[0])
        if first >= 224 and first < 240:
            self._multicast = True

        if self._multicast:
            self._bindPort = port

    async def open(self):
        loop = asyncio.get_running_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)

        if self._multicast:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        sock.bind((self._bind, self._bindPort))
        sock.setblocking(False)

        self._transport, _ = await loop.create_datagram_endpoint(
            asyncio.DatagramProtocol, sock=sock
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
