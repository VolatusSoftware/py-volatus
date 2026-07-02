import time
import asyncio
import socket
from enum import Enum
from collections.abc import Callable, Awaitable

from volatus.config import VolatusConfig, NodeConfig

from ..proto.tcp_payload_pb2 import *
from ..proto.tcp_reply_pb2 import *
from ..proto.tcp_client_hello_pb2 import *
from ..proto.tcp_server_hello_pb2 import *
from ..proto.tcp_client_list_pb2 import *

__all__ = [
    'TCPMessaging',
    'MessageHandler'
]

class ClientState(Enum):
    UNKNOWN = 0
    IDLE = 1
    CONNECTING = 2
    CONNECTED = 3
    CLOSING = 4
    ERROR = 5
    SHUTDOWN = 6

    def __str__(self):
        return f'{self.name}'

class ServerState(Enum):
    UNKNOWN = 0
    IDLE = 1
    LISTENING = 2
    CLOSING = 3
    ERROR = 4
    SHUTDOWN = 5

    def __str__(self):
        return f'{self.name}'

class TCPAction(Enum):
    UNKNOWN = 0
    OPEN = 1
    CLOSE = 2
    SHUTDOWN = 3

    def __str__(self):
        return f'{self.name}'

class ClientInfo:
    def __init__(self, address: tuple[str, int]):
        self.address = address

type MessageHandler = Callable[[TcpPayload, "TCPMessaging"], Awaitable[None]]

class MessageHandlers:
    def __init__(self, msgType: str):
        self.type = msgType

        # str = taskID
        self._handlers: dict[str, MessageHandler] = dict()

    def addHandler(self, taskID: str, handler: MessageHandler):
        self._handlers[taskID] = handler

    async def dispatch(self, payload: TcpPayload, msg: "TCPMessaging"):
        handler = self._handlers.get(payload.task_id)

        if handler:
            await handler(payload, msg)

class TCPMessaging:
    def __init__(self, address: str, port: int, server: bool, vCfg: VolatusConfig, nodeCfg: NodeConfig, seqFunc: Callable[[], int], appVersion: str):
        self.address = address
        self.port = port
        self.server = server
        self.state: str = 'UNKNOWN'
        self.vCfg = vCfg
        self.nodeCfg = nodeCfg
        self.connected = False
        self.appVersion = appVersion

        self.id = nodeCfg.id

        self._actionQ: asyncio.Queue[TCPAction] = asyncio.Queue()
        self._sendQueue: asyncio.Queue[TcpPayload] = asyncio.Queue()
        self._readQueue: asyncio.Queue[TcpPayload] = asyncio.Queue()
        self._clients: dict[str, TcpClientInfo] = dict()

        self._task: asyncio.Task = None
        self._reader: asyncio.StreamReader = None
        self._writer: asyncio.StreamWriter = None

        self._seqFunc = seqFunc

        # msgType, Handlers
        self._handlers: dict[str, MessageHandlers] = dict()

        if self.server:
            raise ValueError('Server mode is not implemented in Python yet.')

    def start(self):
        self._task = asyncio.create_task(self._clientLoop())

    def open(self):
        self._actionQ.put_nowait(TCPAction.OPEN)

    def close(self):
        self._actionQ.put_nowait(TCPAction.CLOSE)

    def shutdown(self):
        self._actionQ.put_nowait(TCPAction.SHUTDOWN)

    def register(self, handler: MessageHandler, msgType: str, taskID: str):
        handlers = self._handlers.get(msgType)

        if not handlers:
            handlers = MessageHandlers(msgType)
            self._handlers[msgType] = handlers

        handlers.addHandler(taskID, handler)

    def replyMsg(self, original: TcpPayload, msgType: str, payload: bytes) -> int:
        reply = TcpReply()
        reply.payload = payload
        reply.request_sequence = original.sequence
        reply.request_timestamp = original.timestamp

        self.sendMsg(original.source_id, "v:TcpReply", reply.SerializeToString())

    def sendMsg(self, target: str | int, msgType: str, payload: bytes, task: str = '') -> int:
        targetId: int
        if type(target) == int:
            targetId = target
        elif type(target) == str:
            cluster = self.vCfg.lookupClusterByName(self.nodeCfg.clusterName)
            targetId = cluster.lookupTargetGroupId(target)
            if not targetId:
                targetId = self.vCfg.lookupNodeByName(target).id
        else:
            raise ValueError('Target must be target name string or target ID int.')

        if targetId:
            toSend = TcpPayload()
            toSend.target_node = targetId
            toSend.source_id = self.id
            toSend.type = msgType
            toSend.task_id = task
            toSend.payload = payload
            self._sendQueue.put_nowait(toSend)

        return self._sendQueue.qsize()
    
    async def _dispatch(self, payload: TcpPayload):
        handler = self._handlers.get(payload.type)
        if handler:
            await handler.dispatch(payload, self)

    async def _clientLoop(self):
        clientHello = TcpClientHello()
        clientHello.node_id = self.nodeCfg.id
        clientHello.system = self.vCfg.system.name
        clientHello.cluster = self.nodeCfg.clusterName
        clientHello.node_name = self.nodeCfg.name
        clientHello.config_version = str(self.vCfg.version)
        clientHello.node_alias = socket.gethostname()
        clientHello.config_hash = self.vCfg.hash
        clientHello.app_version = self.appVersion
        helloPayload = clientHello.SerializeToString()

        state = ClientState.IDLE
        self.state = str(state)

        shutdown = False
        stay_open = False

        readPayload = TcpPayload()

        while not shutdown:
            # check actionQ for commands
            while not self._actionQ.empty():
                action = self._actionQ.get_nowait()
                match action:
                    case TCPAction.OPEN:
                        if state == ClientState.IDLE:
                            stay_open = True
                            state = ClientState.CONNECTING
                            self.state = str(state)

                    case TCPAction.CLOSE:
                        if state != ClientState.IDLE:
                            stay_open = False
                            state = ClientState.CLOSING
                            self.state = str(state)

                    case TCPAction.SHUTDOWN:
                        shutdown = True
                        if state == ClientState.CONNECTED:
                            state = ClientState.CLOSING
                            self.state = str(state)

            # flush the queue while not connected
            if state != ClientState.CONNECTED:
                while not self._sendQueue.empty():
                    self._sendQueue.get_nowait()

            if state == ClientState.CONNECTING:
                try:
                    self._reader, self._writer = await asyncio.wait_for(
                        asyncio.open_connection(self.address, self.port),
                        timeout=5
                    )

                    # connection handshake
                    await self.__sendSized(helloPayload)

                    serverPayload = await self.__recvSized(10)
                    if serverPayload:
                        serverHello = TcpServerHello()
                        serverHello.ParseFromString(serverPayload)
                        if serverHello.status == ConnectStatus.STATUS_SUCCESS:
                            state = ClientState.CONNECTED
                            self.state = str(state)
                        else:
                            raise RuntimeError(
                                f'Connection error {serverHello.status} from server, aborting.'
                            )
                except Exception as e:
                    await asyncio.sleep(0.5)

            if state == ClientState.CONNECTED:
                # check for messages to send
                while not self._sendQueue.empty():
                    payload = self._sendQueue.get_nowait()
                    payload.timestamp = time.time_ns()
                    payload.sequence = self._seqFunc()

                    try:
                        await self.__sendSized(payload.SerializeToString())
                    except Exception:
                        state = ClientState.CLOSING
                        self.state = str(state)
                        break

                # check for receieved messages
                while True:
                    try:
                        readPayload.ParseFromString(await self.__recvSized(0.05))
                    except asyncio.TimeoutError:
                        break
                    except Exception as e:
                        state = ClientState.CLOSING
                        self.state = str(state)
                        break

                    if readPayload.type.startswith('v:'):
                        # built-in messages start with 'v:'
                        match readPayload.type:
                            case "v:ClientList":
                                clients = TcpClientList.FromString(readPayload.payload)
                                clientDict: dict[str, list[TcpClientInfo]] = dict()
                                for client in clients.clients:
                                    nodeList = clientDict.get(client.node_name)
                                    if nodeList:
                                        nodeList.apped(client)
                                    else:
                                        clientDict[client.node_name] = [client]

                                print(clientDict)
                                self._clients = clientDict
                                break
                    else:
                        await self._dispatch(readPayload)

            if state == ClientState.CLOSING:
                if self._writer:
                    try:
                        self._writer.close()
                        await self._writer.wait_closed()
                    except Exception:
                        pass
                    self._writer = None
                    self._reader = None

                if stay_open and not shutdown:
                    state = ClientState.CONNECTING
                    self.state = str(state)
                elif shutdown:
                    state = ClientState.SHUTDOWN
                    self.state = str(state)

            self.connected = state == ClientState.CONNECTED

            # yield control to other tasks
            await asyncio.sleep(0)

    async def __sendSized(self, payload: bytes):
        l = len(payload)
        lb = l.to_bytes(4, 'little')
        self._writer.write(lb + payload)
        await self._writer.drain()

    async def __recvSized(self, timeout: float) -> bytes | None:
        sizeBytes = await asyncio.wait_for(self._reader.readexactly(4), timeout)
        if len(sizeBytes) == 0:
            return None
        size = int.from_bytes(sizeBytes, 'little')
        recvBytes = await self._reader.readexactly(size)
        return recvBytes
    
    def lookupNode(self, nodeName: str) -> list[TcpClientInfo] | None:
        return self._clients.get(nodeName)

    def isConnected(self) -> bool:
        return self.connected
    
    def nextMessage(self) -> TcpPayload | None:
        try:
            payload = self._readQueue.get_nowait()
            return payload
        except asyncio.QueueEmpty:
            return None
