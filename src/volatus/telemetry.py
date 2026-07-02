import time
import asyncio
from enum import Enum
from collections.abc import Callable
from crccheck.crc import Crc32Mpeg2

from volatus.proto import group_data_pb2

from .config import (
    Cfg,
    GroupConfig,
    ChannelConfig,
    EndpointConfig,
    TelemetryConfig,
    TelemetryRouting,
)
from volatus.vecto.UDP import UdpReader, UdpWriter
from volatus.proto import string_data_pb2
from volatus.proto.group_data_pb2 import *

__all__ = ["Telemetry", "ChannelGroup", "ChannelValue"]


class ChannelValue:
    def __init__(self, chanCfg: ChannelConfig):
        self.name = chanCfg.name
        self.value = chanCfg.defaultValue
        self.time_ns = 0

    def update(self, value, timestamp: int):
        self.value = value
        if timestamp:
            self.time_ns = timestamp
        else:
            self.time_ns = time.time_ns()


class ChannelGroup:
    def __init__(self, groupCfg: GroupConfig):
        self._channel: dict[str, ChannelValue] = dict()
        self.config = groupCfg
        self.name = groupCfg.name
        self.time_ns = 0

        self._chanIndex: dict[str, int] = dict()
        self._channels: list[ChannelValue] = []
        self._count = 0

        # channel order is by alphabetical name
        channels = dict(sorted(groupCfg.channels.items()))

        names = []
        i: int = 0
        for chanCfg in channels.values():
            chan = ChannelValue(chanCfg)
            self._channels.append(chan)
            self._channel[chan.name] = chan
            self._chanIndex[chanCfg.name] = i
            names.append(chan.name)
            i += 1

        self._count = i

        nameCsv = ",".join(names)
        nameData = nameCsv.encode()
        self.namesCrc = Crc32Mpeg2.calc(nameData)

    def __eq__(self, other) -> bool:
        if isinstance(other, ChannelGroup):
            return self.name == other.name
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.name)

    def chanByName(self, chanName: str) -> ChannelValue:
        ch = self._channel.get(chanName)
        if not ch:
            raise RuntimeError(f"Channel {chanName} not found.")

        return ch

    def chanIndex(self, chanName: str) -> int | None:
        return self._chanIndex.get(chanName)

    def chanByIndex(self, chanIndex: int) -> ChannelValue | None:
        return self._channels[chanIndex]

    def valueByIndex(self, chanIndex: int) -> str | float | None:
        return self._channels[chanIndex].value()

    def updateValues(self, values: list[str | float], time_ns: int = None):
        if not time_ns:
            time_ns = time.time_ns()

        if len(values) != self._count:
            raise ValueError()

        for i, chan in enumerate(self._channels):
            chan.update(values[i], time_ns)  # TODO check value order

        self._time_ns = time_ns

    def allValues(self) -> tuple[list[str | float], int]:
        """
        Returns the current values stored by the group of channels

        Return: tuple[values: list[str | float | None], time_ns: int]
        """
        vals = []
        for chan in self._channels:
            vals.append(chan.value)

        return vals, self._time_ns


class Subscriber:
    def __init__(self, endpt: EndpointConfig, bindAddress: str = "0.0.0.0"):
        self._endpoint = endpt
        self._pendingGroups: asyncio.Queue[ChannelGroup] = asyncio.Queue()
        self._reader = UdpReader(endpt.address, endpt.port, bindAddress)
        self._groups: dict[str, ChannelGroup] = dict()
        self._close = False
        self._task: asyncio.Task = None

    async def start(self):
        await self._reader.join()
        self._task = asyncio.create_task(self._readLoop())

    def addGroup(self, group: ChannelGroup):
        # if group.config.publishConfig != self._endpoint:
        #    raise ValueError(f'Group {group.name} does not match subscriber endpoint of {str(self._endpoint)}')

        self._pendingGroups.put_nowait(group)

    def close(self):
        self._close = True

    async def _readLoop(self):
        groupData = group_data_pb2.GroupData()
        stringData = string_data_pb2.StringData()

        while not self._close:
            # check for pending group additions
            while not self._pendingGroups.empty():
                group = self._pendingGroups.get_nowait()
                self._groups[group.name] = group

            # read payload
            try:
                udpPayload = await self._reader.readUdpPayload()
                if not udpPayload:
                    # disconnected, try rejoining multicast
                    self._reader.close()
                    await self._reader.join()
                    continue

                match udpPayload.type:
                    case "v:GroupData":
                        # numeric data
                        groupData.ParseFromString(udpPayload.payload)
                        group = self._groups.get(groupData.group_name)
                        if group:
                            group.updateValues(
                                groupData.scaled_data, groupData.data_timestamp
                            )

                    case "v:StringData":
                        stringData.ParseFromString(udpPayload.payload)
                        group = self._groups.get(stringData.group_name)
                        if group:
                            group.updateValues(
                                stringData.strings, stringData.data_timestamp
                            )

            except TimeoutError:
                pass

        self._reader.close()


class GroupPublisher:
    def __init__(
        self,
        group: ChannelGroup,
        endpt: EndpointConfig,
        writer: UdpWriter,
        seqFunc: Callable[[], int],
    ):
        self.group = group
        self._endpt = endpt
        self._writer = writer
        self._seqFunc: Callable[[], int] = seqFunc

    def publish(self):
        values, time_ns = self.group.allValues()
        msg = GroupData()
        msg.data_timestamp = time_ns
        msg.group_name = self.group.name
        msg.names_crc = self.group.namesCrc
        msg.scaled_data.extend(values)
        self._writer.sendPayload(
            msg.SerializeToString(), "v:GroupData", self._seqFunc()
        )


class Telemetry:
    def __init__(
        self,
        telemCfg: TelemetryConfig,
        nodeId: int,
        bindAddress: str,
        seqFunc: Callable[[], int],
    ):
        self._values = dict()
        self._subscribers: dict[EndpointConfig, Subscriber] = dict()
        self._subGroups = dict()
        self._telemCfg = telemCfg
        self._nodeId = nodeId
        self._bindAddress = bindAddress
        self._pubGroups: dict[str, GroupPublisher] = dict()
        self._writers: dict[EndpointConfig, UdpWriter] = dict()
        self._seqFunc: Callable[[], int] = seqFunc

    async def subscribe(
        self, groupCfg: GroupConfig, timeout_s: float = None
    ) -> tuple[ChannelGroup, bool]:
        """Subscribes to a group based on its configuration.

        :param groupCfg: The configuration of the group to subscribe to. Must include publish configuration.
        :type groupCfg: GroupConfig
        :param timeout_s: Wait up to this amount of time for data to arrive after subscribing, defaults to None
        :type timeout_s: int, optional
        :raises ValueError: The group config does not have a publish configuration.
        :return: The group that was subscribed to and true if data has been received before the timeout.
        :rtype: tuple[ChannelGroup, bool]
        """
        # check to see if group already exists
        group = self._subGroups.get(groupCfg.name)
        if not group:
            endpt = self._telemCfg.endpt

            if self._telemCfg.routing == TelemetryRouting.Multicast:
                endpt = groupCfg.publishConfig

            group = ChannelGroup(groupCfg)
            self._subGroups[group.name] = group

            if not endpt:
                raise ValueError(
                    f"No valid telemetry config for Group {groupCfg.name()} and it cannot be subscribed to. Ensure a Unicast telemetry config is provided or a group publish config."
                )

            if endpt in self._subscribers:
                sub = self._subscribers[endpt]
                sub.addGroup(group)
            else:
                sub = Subscriber(endpt, self._bindAddress)
                self._subscribers[endpt] = sub
                sub.addGroup(group)
                await sub.start()

        # get first channel to check for data
        chan = group.chanByIndex(0)
        hasData = chan.time_ns > 0

        if timeout_s is not None and not hasData:
            start = time.time()

            # chan.time_ns is updated asynchronously via the udp read loop
            while time.time() - start < timeout_s and chan.time_ns == 0:
                await asyncio.sleep(0.01)

            hasData = chan.time_ns > 0

        return (group, hasData)

    async def registerForPublish(self, groupCfg: GroupConfig) -> ChannelGroup:
        """Given a GroupConfig instance, prepares for the publishing of that group and returns the group to be published.

        :param groupCfg: The configuration object describing the group to publish.
        :type groupCfg: ChannelConfig
        :return: The live data group that holds updated values before publishing.
        :rtype: ChannelGroup
        """

        # Check if group is already registered and return early if it exists
        pubGroup = self._pubGroups.get(groupCfg.name)
        if pubGroup != None:
            return pubGroup.group

        group = ChannelGroup(groupCfg)
        endpt = groupCfg.publishConfig

        if self._telemCfg.routing == TelemetryRouting.Unicast:
            endpt = self._telemCfg.endpt

        writer = self._writers.get(endpt)
        if not writer:
            if self._telemCfg.routing == TelemetryRouting.Unicast:
                writer = UdpWriter(endpt.address, endpt.port, self._nodeId, self._bindAddress)
            await writer.open()

        pubGroup = GroupPublisher(group, endpt, writer, self._seqFunc)
        self._pubGroups[group.name] = pubGroup

        return group

    def publish(self, group: ChannelGroup):
        pubGroup = self._pubGroups.get(group.name)

        if not pubGroup:
            raise RuntimeError(f'Cannot publish group "{group.name}". Either the group was not registered for publishing or it was created incorrectly.')
        
        pubGroup.publish()

    def shutdown(self):
        for sub in self._subscribers.values():
            sub.close()
        
        for pub in self._writers.values():
            pub.close()
