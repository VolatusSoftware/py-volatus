import time
import asyncio
import ipaddress

from .config import Cfg, GroupConfig, ChannelConfig, EndpointConfig, VolatusConfig, NodeConfig
from .vecto.UDP import MulticastReader, MulticastWriter
from .vecto import util
from .vecto.proto import discovery_pb2

class DiscoveryService:

    def __init__(self, vCfg: VolatusConfig, nodeCfg: NodeConfig):
        self._cluster = vCfg.lookupClusterByName(nodeCfg.clusterName)
        self._nodes: dict[str, discovery_pb2.Discovery] = dict()
        self._nodeCfg = nodeCfg
        self._vCfg = vCfg
        self._close = False

        self._reader = MulticastReader(self._cluster.discovery.address,
                                       self._cluster.discovery.port,
                                       nodeCfg.network.bindAddress)

        self._writer = MulticastWriter(self._cluster.discovery.address,
                                       self._cluster.discovery.port,
                                       nodeCfg.id,
                                       nodeCfg.network.bindAddress)

        self._readerTask: asyncio.Task = None
        self._writerTask: asyncio.Task = None

    async def start(self):
        await self._reader.join()
        await self._writer.open()
        self._readerTask = asyncio.create_task(self._readerLoop())
        self._writerTask = asyncio.create_task(self._writerLoop())

    def shutdown(self):
        self._close = True

    def lookupNodeByName(self, nodeName: str) -> discovery_pb2.Discovery:
        return self._nodes.get(nodeName)

    async def _writerLoop(self):
        interval = self._nodeCfg.network.announceInterval

        bindAddress = util.resolveAddress(self._nodeCfg.network.bindAddress)

        if bindAddress == '0.0.0.0':
            bindAddress = util.localIPs()[0]

        discovery = discovery_pb2.Discovery()
        discovery.node_id = self._nodeCfg.id
        discovery.name = self._nodeCfg.name
        discovery.ip = int(ipaddress.ip_address(bindAddress))
        discovery.system = self._vCfg.system.name
        discovery.cluster = self._nodeCfg.clusterName
        discovery.cfg_version = str(self._vCfg.version)

        if self._nodeCfg.network.httpPort:
            httpService = discovery_pb2.Service()
            httpService.type = discovery_pb2.ServiceType.SERVICETYPE_HTTPSERVER
            httpService.port = self._nodeCfg.network.httpPort

            discovery.services.append(httpService)

        payload = discovery.SerializeToString()

        lastAnnounce = 0

        while not self._close:
            now = time.time()
            if now - lastAnnounce > interval:
                self._writer.sendPayload(payload, 'v:Discovery', 0)
                lastAnnounce = now

            await asyncio.sleep(0.2)

        self._writer.close()

    async def _readerLoop(self):
        discovery = discovery_pb2.Discovery()

        while not self._close:
            try:
                udpPayload = await self._reader.readUdpPayload()
                if not udpPayload:
                    self._reader.close()
                    await self._reader.join()
                    continue

                match udpPayload.type:
                    case 'v:Discovery':
                        discovery.ParseFromString(udpPayload.payload)
                        self._nodes[discovery.name] = discovery

            except TimeoutError:
                pass

        self._reader.close()
