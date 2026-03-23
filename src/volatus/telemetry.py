import time
import asyncio
from enum import Enum

from .config import Cfg, GroupConfig, ChannelConfig, EndpointConfig
from .vecto.UDP import MulticastReader, MulticastWriter
from .vecto.proto import group_data_pb2, string_data_pb2

__all__ = [
    'Telemetry',
    'ChannelGroup',
    'ChannelValue'
]

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

        #channel order is by alphabetical name
        channels = dict(sorted(groupCfg.channels.items()))

        i:int = 0
        for chanCfg in channels.values():
            chan = ChannelValue(chanCfg)
            self._channels.append(chan)
            self._channel[chan.name] = chan
            self._chanIndex[chanCfg.name] = i
            i += 1

        self._count = i

    def __eq__(self, other) -> bool:
        if isinstance(other, ChannelGroup):
            return self.name == other.name
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(self.name)

    def chanByName(self, chanName: str) -> ChannelValue | None:
        return self._channel.get(chanName)

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
            chan.update(values[i], time_ns) #TODO check value order

        self._time_ns = time_ns

    def allValues(self) -> tuple[list[str | float], int]:
        """
        Returns the current values stored by the group of channels

        Return: tuple[values: list[str | float | None], time_ns: int]
        """
        vals = []
        for chan in self._channels:
            vals.append(chan.value())

        return vals, self._time_ns

class Subscriber:
    def __init__(self, endpt: EndpointConfig, bindAddress: str = '0.0.0.0'):
        self._endpoint = endpt
        self._pendingGroups: asyncio.Queue[ChannelGroup] = asyncio.Queue()
        self._reader = MulticastReader(endpt.address, endpt.port, bindAddress)
        self._groups: dict[str, ChannelGroup] = dict()
        self._close = False
        self._task: asyncio.Task = None

    async def start(self):
        await self._reader.join()
        self._task = asyncio.create_task(self._readLoop())

    def addGroup(self, group: ChannelGroup):
        if group.config.publishConfig != self._endpoint:
            raise ValueError(f'Group {group.name} does not match subscriber endpoint of {str(self._endpoint)}')

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
                    case 'v:GroupData':
                        # numeric data
                        groupData.ParseFromString(udpPayload.payload)
                        group = self._groups.get(groupData.group_name)
                        if group:
                            group.updateValues(groupData.scaled_data, groupData.data_timestamp)

                    case 'v:StringData':
                        stringData.ParseFromString(udpPayload.payload)
                        group = self._groups.get(stringData.group_name)
                        if group:
                            group.updateValues(stringData.strings, stringData.data_timestamp)

            except TimeoutError:
                pass

        self._reader.close()


class Telemetry:
    def __init__(self):
        self._values = dict()
        self._subscribers: dict[EndpointConfig, Subscriber] = dict()
        self._groups = dict()

    async def subscribeToGroupCfg(self, groupCfg: GroupConfig,
                            timeout_s: float = None,
                            bindAddress: str = '0.0.0.0') -> tuple[ChannelGroup, bool]:
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
        group = self._groups.get(groupCfg.name)
        if not group:
            endpt = groupCfg.publishConfig

            group = ChannelGroup(groupCfg)
            self._groups[group.name] = group

            if not endpt:
                raise ValueError(f'Group {groupCfg.name()} does not have a publish config and cannot be subscribed to.')

            if endpt in self._subscribers:
                sub = self._subscribers[endpt]
                sub.addGroup(group)
            else:
                sub = Subscriber(endpt, bindAddress)
                self._subscribers[endpt] = sub
                sub.addGroup(group)
                await sub.start()

        #get first channel to check for data
        chan = group.chanByIndex(0)
        hasData = chan.time_ns > 0

        if timeout_s is not None and not hasData:
            start = time.time()

            while time.time() - start < timeout_s and chan.time_ns == 0:
                await asyncio.sleep(0.01)

            hasData = chan.time_ns > 0

        return (group, hasData)

    def shutdown(self):
        for sub in self._subscribers.values():
            sub.close()
