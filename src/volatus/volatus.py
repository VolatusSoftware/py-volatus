"""The core module containing the Volatus class to be used for handling configs and system interactions."""

from pathlib import Path
from collections.abc import Callable
from datetime import datetime
from fastapi import FastAPI, APIRouter
from enum import Enum
from typing import final
from abc import ABC, abstractmethod
from dataclasses import dataclass

import uvicorn
import os
import signal
import ipaddress
import json
import time
import asyncio
import aiohttp
import aiofiles
import configparser

from volatus.telemetry import Telemetry, ChannelGroup, ChannelValue
from volatus.config import (
    VolatusConfig,
    NodeConfig,
    ConfigLoader,
    ClusterConfig,
    GroupConfig,
    TaskConfig,
)
from volatus.vecto.TCP import TCPMessaging, MessageHandler
from volatus.proto.cmd_digital_pb2 import CmdDigital, CmdDigitalMultiple
from volatus.proto.cmd_analog_pb2 import CmdAnalog, CmdAnalogMultiple
from volatus.proto.start_log_pb2 import StartLog
from volatus.proto.stop_log_pb2 import StopLog
from volatus.proto.event_pb2 import EventLevel, Event, Events
from volatus.proto.tcp_payload_pb2 import *

class Identity:
    def type(self) -> str:
        return type(self).__name__

    @abstractmethod
    def value(self) -> str:
        raise NotImplementedError

class ModuleIdentity(Identity):
    def __init__(self, module_name: str):
        self.module_name = module_name
    
    def value(self) -> str:
        return self.module_name
    
class Module:
    @final
    def __init__(self, task_config: TaskConfig, v: "Volatus", name: str = None):
        self.v = v
        self.task_config = task_config
        if name:
            self.name = name
        else:
            self.name = task_config.name

        self._created_groups: dict[str, ChannelGroup] = {}

        self._task: asyncio.Task = None

        self.module_init()

    def launch(self):
        self.module_init()
        self._task = asyncio.create_task(self.module_loop())

    def stop(self):
        if self._task:
            self._task.cancel()

    @abstractmethod
    def module_init(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def module_loop(self):
        raise NotImplementedError

    def module_ids(self) -> list[Identity]:
        return []

    @staticmethod
    @abstractmethod
    def module_type() -> str:
        raise NotImplementedError

    def report_event(self, msg: str, level: EventLevel = EventLevel.EVENTLEVEL_INFO):
        self.v.reportEvent("Events", level, self.name, msg)

    def report_error(self, msg: str, err_code: int = 1, err_detail: str = ""):
        self.v.reportError("Events", err_code, err_detail, self.name, msg)

    def register_message_handler(self, msg_type: str, handler: MessageHandler):
        self.v.registerMessageHandler(msg_type, self.name, handler)

    async def create_group(self, group_name: str, timeout: float = None) -> ChannelGroup:
        if group_name in self.task_config.groups:
            # own group, return group created for publishing
            return await self.v.registerForPublish(group_name)
        else:
            return (await self.v.subscribe(group_name, timeout))[0]

_module_types: dict[str, Module] = {}
"""Stores registered module types for automatically launching modules from vjson config."""

def register_module(mod_cls):
    mod_type = mod_cls.module_type()
    _module_types[mod_type] = mod_cls

    print(f"Registering module type '{mod_type}' for class '{mod_cls.__name__}'.")

def lookup_module_type(type: str) -> type[Module]:
    return _module_types.get(type)

class LogState(Enum):
    Unknown = 0
    Idle = 1
    Starting = 2
    Logging = 3
    Stopping = 4

    def __str__(self):
        return f"{self.name}"


class LogStatus:

    def __init__(self, state: LogState, log: str):
        self.state = state
        self.log = log

    def __str__(self) -> str:
        return json.dumps(self.__dict__)


class VCommand:
    """Constructed command that is ready to be sent to a Volatus system."""

    def __init__(
        self,
        targetName: str,
        type: str,
        payload: bytes,
        sendFunc: Callable[[str, str, bytes, int, str], None],
        taskName: str = "",
    ):
        """Initializes a new command that is ready to be sent.

        :param targetName: The name of the target to send the command to. Can be a node name or a targetGroup name.
        :type targetName: str
        :param type: The message type string use to infer the message type by the recipient.
        :type type: str
        :param payload: The serialized protobuf message used as the command data.
        :type payload: bytes
        :param sendFunc: A reference to the function that sends the message out over TCP. Expected to be the send function of the TCP class.
        :type sendFunc: Callable[[str, str, bytes, int, str], None]
        :param taskName: The target task for the command, defaults to '' which requires tasks to be subscribed to the specific message type.
        :type taskName: str, optional
        """
        self._targetName = targetName
        self._type = type
        self._payload = payload
        self._taskName = taskName
        self._sendFunc = sendFunc

    def send(self):
        """Sends the command over TCP as initialized."""
        self._sendFunc(self._targetName, self._type, self._payload, self._taskName)


class StartLogCommand(VCommand):
    """A prepared command to start logging across a set of target nodes that can be sent with send()"""

    def __init__(
        self,
        targetName: str,
        testName: str,
        sendFunc: Callable[[str, str, bytes, int, str], None],
        startedBy: str,
        timestamp: str = "",
    ):
        self._sendFunc = sendFunc
        self._targetName = targetName
        self._timestamp = timestamp

        self._cmd = StartLog()
        self._cmd.series = testName
        self._cmd.started_by = startedBy

    def send(self):
        if not self._timestamp:
            self._timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")

        self._cmd.timestamp = self._timestamp

        count = self._sendFunc(
            self._targetName,
            "start_log",
            self._cmd.SerializeToString(),
            "",
        )

        print(f"Send Q size: {count}")

type VolatusApp = Callable[[Volatus], None]

async def _ini_app_run(method: VolatusApp, ini_path: str = 'volatus.ini', app_version: str = '0.0.0'):
    async with Volatus.from_ini(ini_path, app_version) as v:
        await method(v)

class Volatus:
    """The main API class for interacting with Volatus configs and systems."""

    def __init__(
        self,
        configPath: Path,
        systemName: str,
        clusterName: str,
        nodeName: str,
        appVersion: str = "",
        connectionTimeout: float = 10.0,
    ):
        """Prepares to interact with a Volatus system with the provided configuration.

        The python script/app is expected to have a node entry in the specified configuration file.

        :param configPath: The path to the configuration file that described the Volatus system.
        :type configPath: Path
        :param systemName: The system name the script is expecting to interact with. This is used as validation that the script is intended for the configured system.
        :type systemName: str
        :param clusterName: Teh cluster within the system that the script can communicate with. Most Volatus systems will only have a single cluster.
        :type clusterName: str
        :param nodeName: The name of the python script within the configuration file.
        :type nodeName: str
        :raises ValueError: The specified systemName was not found in the configuration.
        :raises ValueError: The specified clusterName or nodeName was not found in the configuration.
        """

        self.systemName: str = systemName
        """The name of the system in the configuration to validate the correct system is being referenced."""

        self.clusterName: str = clusterName
        """The name of the cluster this app belongs to in the configuration."""

        self.nodeName: str = nodeName
        """The name of the node (application) to use from the configuration."""

        self.config: VolatusConfig = ConfigLoader.load(configPath)
        """The configuration from the configPath argument."""

        self.path: Path = configPath

        self.appVersion = appVersion

        self._cluster: ClusterConfig
        self._node: NodeConfig
        self._telemetry: Telemetry
        self._tcp: TCPMessaging
        self._connectionTimeout = connectionTimeout

        self._ids: dict[str, dict[str, str]] = {}
        """Stores lookup of identity type -> identity value -> module name"""

        self._tasks: dict[str, Module] = {}

        self._seq = 0

        cfgSystemName = self.config.system.name

        if systemName != cfgSystemName:
            raise ValueError(
                f'Created config object for "{systemName}" system but config loaded is for "{cfgSystemName}".'
            )

        self._cluster = self.config.lookupClusterByName(clusterName)
        if self._cluster:
            self._node = self._cluster.lookupNodeByName(nodeName)

        if not self._node:
            raise ValueError(
                f'Unable to find node "{nodeName}" in cluster "{clusterName}".'
            )

    @staticmethod
    def from_ini(
        ini_path: Path | str = 'volatus.ini',
        app_version: str = '0.0.0',
        connect_timeout: float = 10.0,
    ) -> "Volatus":
        ini = configparser.ConfigParser()
        ini.read(ini_path)
        v_ini = ini['Volatus']

        system_name = v_ini['System']
        cluster_name = v_ini['Cluster']
        node_name = v_ini['Node']
        cfg_path = Path(v_ini['Config'])

        if not cfg_path.is_absolute():
            if isinstance(ini_path, str):
                ini_path = Path(ini_path)

            cfg_path = (ini_path.parent / cfg_path).resolve()

        return Volatus(cfg_path, system_name, cluster_name, node_name, app_version, connect_timeout)

    @staticmethod
    def main(async_main):
        asyncio.run(async_main())

    @staticmethod
    def ini_app(async_app, ini_path: str = 'volatus.ini', app_version: str = '0.0.0'):
        asyncio.run(_ini_app_run(async_app, ini_path, app_version))


    async def __initFromConfig(self):
        node = self._node

        if node.network.httpPort:
            await self.__startHTTP()

        if node.network.tcp:
            self.__startTCP()

        self.__createTelemetry()

    def __createTelemetry(self):
        self._telemetry = Telemetry(
            self._cluster.telemetry,
            self._node.id,
            self._node.network.bindAddress,
            self.__nextSeq,
        )

    def __startTCP(self):
        tcpCfg = self._node.network.tcp

        self._tcp = TCPMessaging(
            tcpCfg.address,
            tcpCfg.port,
            tcpCfg.server,
            self.config,
            self._node,
            self.__nextSeq,
            self.appVersion,
        )
        self._tcp.start()
        self._tcp.open()

    def registerMessageHandler(
        self, msgType: str, taskID: str, handler: MessageHandler
    ):
        if not self._tcp:
            raise RuntimeError("Messaging is not configured.")

        self._tcp.register(handler, msgType, taskID)

    async def wait_terminate(self):
        await asyncio.Event().wait()

    async def __startHTTP(self):
        self._http = FastAPI()
        self._http.add_api_route("/config/info", self._httpConfigInfo, methods=["GET"])

        httpConfig = uvicorn.Config(
            self._http, host="0.0.0.0", port=self._node.network.httpPort
        )
        self._httpServer = uvicorn.Server(httpConfig)
        self._httpTask = asyncio.create_task(self._httpServer.serve())
        await asyncio.sleep(0.5)  # give uvicorn server a chance to start

    def addHttpRouter(self, router: APIRouter):
        return self._http.include_router(router)

    def _httpConfigInfo(self):
        return {
            "System": self.config.system.name,
            "Cluster": self._node.clusterName,
            "Node": self._node.name,
            "Path": str(self.path),
            "Version": str(self.config.version),
            "Hash": self.config.hash.upper(),
        }

    async def __aenter__(self):
        await self.__initFromConfig()

        if self._connectionTimeout > 0:
            await self.waitForConnection()

        await self.launch_configured_tasks()

        return self

    async def __aexit__(self, type, value, traceback):
        await self.shutdown()

    async def launch_configured_tasks(self):
        for name, task_cfg in self._node.tasks.items():
            task_type = task_cfg.lookupMetaValue("VL_Task_Type")
            if task_type:
                mod_cls = lookup_module_type(task_type)
                if not mod_cls:
                    print(f"Unknown task type '{task_type}' for task '{name}', ignoring.")
                    continue

                module = mod_cls(task_cfg, self, name)
                await self.launch_module(module)

    async def launch_module(self, module: Module):
        mod_type = module.module_type() #staticmethod can be called before init

        print(f"Launching module '{module.name}' as type '{mod_type}'.")
        try:
            module.launch()
            self._tasks[module.name] = module
            ids = module.module_ids()
            ids.append(ModuleIdentity(module.name))

            for id in ids:
                values = self._ids.get(id.type())
                if not values:
                    values = {}
                    self._ids[id.type()] = values

                name = values.get(id.value())
                if name:
                    print(f"Duplicate identity {id.type()}:{id.value()}, ignoring.")
                    continue

                values[id.value()] = module.name

            print(f"Task '{module.name}' launched.")
        except Exception as e:
            print(f"Task '{module.name}' launch failed: {e}")

    def lookup_id[T: Module](self, id: Identity, as_type: type[T] = Module) -> T | None:
        values = self._ids.get(id.type())
        if values:
            name = values.get(id.value())
            if name:
                return self._tasks.get(name)

        return None

    async def lookup_id_timeout[T: Module](self, id: Identity, as_type: type[T] = Module, timeout_s: float = 1) -> T | None:
        module: Module = None
        start = time.time()
        while time.time() - start < timeout_s:
            module = self.lookup_id(id)
            if not module:
                await asyncio.sleep(0.01)

        return module

    async def waitForConnection(self):
        start = time.time()

        while not self.isConnected() and time.time() - start < self._connectionTimeout:
            await asyncio.sleep(0.1)

    def __nextSeq(self) -> int:
        seq = self._seq
        self._seq += 1
        return seq


    def isConnected(self):
        return self._tcp.isConnected()

    async def shutdown(self):
        """Stops all communication tasks managed by the Volatus framework to prepare for reloading configuration or stopping the Python app."""

        for _, task in self._tasks.items():
            task.stop()

        if hasattr(self, "_tcp"):
            self._tcp.shutdown()

        if hasattr(self, "_telemetry"):
            self._telemetry.shutdown()

        if hasattr(self, "_httpServer"):
            self._httpServer.should_exit = True
            if hasattr(self, "_httpTask"):
                await self._httpTask

    def lookupTargetId(self, targetName: str) -> int | None:
        """Looks up the numeric ID used to route a message to the desired node(s).

        Also useful for verifying if a target name is valid; unknown target names return None as the value.
        """
        # check if target is a node
        node = self._cluster.lookupNodeByName(targetName)
        if node:
            return node.id

        # check if target is a targetGroup
        targetGroup = self._cluster.lookupTargetGroupId(targetName)
        return targetGroup

    def nodeIP(self, nodeName: str) -> str | None:
        clients = self._tcp.lookupNode(nodeName)
        if clients:
            return clients[0].address

        return None

    def nodeHttpUrl(self, nodeName: str, urlPath: str) -> str | None:
        cluster = self.config.lookupClusterByName(self._node.clusterName)
        target = cluster.lookupNodeByName(nodeName)
        httpPort = target.network.httpPort

        if not httpPort:
            return None

        ipStr = self.nodeIP(nodeName)

        if not ipStr:
            return None

        ip = ipaddress.ip_address(ipStr)
        return f"http://{ip}:{httpPort}{urlPath}"

    async def requestLogStatus(self, nodeName: str = None) -> dict[str, LogStatus]:
        cluster = self.config.lookupClusterByName(self._node.clusterName)
        nodes = cluster.nodes

        status: dict[str, LogStatus] = dict()
        for nodeName, _ in nodes.items():
            if nodeName != self.nodeName:
                url = self.nodeHttpUrl(nodeName, "/log")

                if not url:
                    continue

                async with aiohttp.ClientSession() as session:
                    async with session.get(url) as response:
                        try:
                            logStatus = json.loads(await response.text())
                        except json.JSONDecodeError:
                            logStatus = dict()

                state = LogState.Unknown
                log = ""

                stateStr = logStatus.get("State")
                if stateStr:
                    state = LogState[stateStr]
                    log = logStatus.get("Log")

                status[nodeName] = LogStatus(state, log)

        return status

    async def waitForLogState(self, state: LogState, timeoutS: float = 5) -> bool:
        start = time.time()
        matched = False
        while not matched:
            status = await self.requestLogStatus()

            matched = len(status) > 0

            for nodeStatus in status.values():
                if nodeStatus.state != state:
                    matched = False
                    break

            if time.time() - start >= timeoutS:
                return False

        return matched

    async def listLogs(self, nodeName: str) -> list[str] | None:
        logs = []
        url = self.nodeHttpUrl(nodeName, "/log/list")

        if not url:
            return None

        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                try:
                    logs = json.loads(await response.text())
                finally:
                    pass

        return logs

    async def prepareLog(
        self, nodeName: str, logName: str, waitUntilDone: bool = True
    ) -> bool | None:
        logs = await self.listLogs(nodeName)
        if not logName in logs:
            return None

        prepUrl = self.nodeHttpUrl(nodeName, f"/log/prepare/{logName}")

        if not prepUrl:
            return None

        statusUrl = self.nodeHttpUrl(nodeName, f"/log/status/{logName}")

        async with aiohttp.ClientSession() as session:
            async with session.get(prepUrl) as response:
                result = await response.text()
                if result != "Preparing":
                    return None

            if waitUntilDone:
                done = False
                while not done:
                    async with session.get(statusUrl) as response:
                        status = await response.text()

                        if status != "In Progress":
                            done = True

                return True

        return False

    async def downloadLog(
        self, nodeName: str, logName: str, localFolder: Path
    ) -> Path | None:
        downloadUrl = self.nodeHttpUrl(nodeName, f"/log/download/{logName}")
        async with aiohttp.ClientSession() as session:
            async with session.get(downloadUrl) as response:
                if response.status != 200:
                    raise aiohttp.ClientError(
                        f"({response.status} - {await response.text()})"
                    )

                filePath = localFolder.joinpath(f"{logName}.zip")
                async with aiofiles.open(filePath, "wb") as file:
                    async for data, _ in response.content.iter_chunks():
                        await file.write(data)

                return filePath

    def createDigitalCommand(self, chanName: str, value: bool) -> VCommand:
        """Prepares a digital command to be sent to a Volatus system.

        Digital commands are typically used to set an output value or trigger a control component.

        :param chanName: The name of the channel to update the value for.
        :type chanName: str
        :param value: The new value to set the channel to.
        :type value: bool
        :raises ValueError: The specified channel name was not found in the system.
        :return: The initialized command ready to be sent.
        :rtype: VCommand
        """
        cmd = CmdDigital()
        cmd.channel = chanName
        cmd.value = value

        chan = self.config.lookupChannelByName(chanName)
        if not chan:
            raise ValueError(f'Unknown channel "{chanName}".')

        targetName = chan.nodeName
        taskName = chan.taskName

        return VCommand(
            targetName,
            "cmd_digital",
            cmd.SerializeToString(),
            self._tcp.sendMsg,
            taskName,
        )

    def createAnalogCommand(self, chanName: str, value: float) -> VCommand:
        """Prepares an analog/numeric command to send to a Volatus system.

        Analog commands are typically used to update analog outputs or change numeric parameters of a control component.

        :param chanName: The name of the channel to update the value of.
        :type chanName: str
        :param value: The new value to set the channel to.
        :type value: float
        :raises ValueError: The specified channel was not found in the system.
        :return: The initialized command ready to be sent.
        :rtype: VCommand
        """
        cmd = CmdAnalog()
        cmd.channel = chanName
        cmd.value = value

        chan = self.config.lookupChannelByName(chanName)
        if not chan:
            raise ValueError(f'Unknown channel "{chanName}"')

        targetName = chan.nodeName
        taskName = chan.taskName

        return VCommand(
            targetName,
            "cmd_analog",
            cmd.SerializeToString(),
            self._tcp.sendMsg,
            taskName,
        )

    def createDigitalMultipleCommand(self, values: list[tuple[str, bool]]) -> VCommand:
        """Creates a command that can update multiple digital values simultaneously.

        This is the multiple version of DigitalCommand. All values specified must belong to the same task.

        :param values: Pairs of channel names and values to update.
        :type values: list[tuple[str, bool]]
        :raises ValueError: A specified channel was not found in the system.
        :raises ValueError: Channels are not all part of the same task.
        :return: The intiialized command ready to be sent.
        :rtype: VCommand
        """
        cmd = CmdDigitalMultiple()

        targetName: str = None
        taskName: str = None

        for chanName, value in values:
            val = cmd.values.add()
            val.channel = chanName
            val.value = value

            chan = self.config.lookupChannelByName(chanName)
            if not chan:
                raise ValueError(f'Unknown channel "{chanName}"')

            if not targetName:
                targetName = chan.nodeName
                taskName = chan.taskName
            else:
                if targetName != chan.nodeName or taskName != chan.taskName:
                    raise ValueError(
                        "Multiple command can only include channels from a single node/task."
                    )

        return VCommand(
            targetName,
            "cmd_digital_multiple",
            cmd.SerializeToString(),
            self._tcp.sendMsg,
            taskName,
        )

    def createAnalogMultipleCommand(self, values: list[tuple[str, float]]) -> VCommand:
        """Prepares a command that can update multiple numeric values simultaneously

        This is the multiple version of AnalogCommand. All channels in this command must belong to the same task.

        :param values: Pairs of channel names and values to update.
        :type values: list[tuple[str, float]]
        :raises ValueError: A specified channel name was not found in the system.
        :raises ValueError: Channels are not all part of the same task.
        :return: The initialized commmand ready to be sent.
        :rtype: VCommand
        """
        cmd = CmdAnalogMultiple()

        targetName: str = None
        taskName: str = None

        for chanName, value in values:
            val = cmd.values.add()
            val.channel = chanName
            val.value = value

            chan = self.config.lookupChannelByName(chanName)
            if not chan:
                raise ValueError(f'Unknown channel "{chanName}"')

            if not targetName:
                targetName = chan.nodeName
                taskName = chan.taskName
            else:
                if targetName != chan.nodeName or taskName != chan.taskName:
                    raise ValueError(
                        "Multiple command can only include channels from a single node/task."
                    )

        return VCommand(
            targetName,
            "cmd_analog_multiple",
            cmd.SerializeToString(),
            self._tcp.sendMsg,
            taskName,
        )

    def createStartLogCommand(
        self, targetName: str, testName: str, startedBy: str, timestamp: str = ""
    ) -> VCommand:
        """Prepare a Start Log command to send to a Volatus system.

        :param targetName: Either the node or targetGroup to send the log command to.
        :type targetName: str
        :param testName: The primary name used for the log.
        :type testName: str
        :param startedBy: The user or source of the start log command.
        :type startedBy: str
        :param timestamp: The string representation of the time of the start log command, should be in basic ISO-8601
            format with second precision, when defaulted to '' it generates a timestamp string when the command is sent.
        :type timestamp: str, optional
        :return: The prepared command ready to be sent with send()
        :rtype: VCommand
        """
        cmd = StartLogCommand(
            targetName,
            testName,
            self._tcp.sendMsg,
            startedBy,
            timestamp,
        )

        return cmd

    def createStopLogCommand(self, targetName: str, reason: str) -> VCommand:
        cmd = StopLog()
        cmd.reason = reason
        return VCommand(
            targetName,
            "stop_log",
            cmd.SerializeToString(),
            self._tcp.sendMsg,
        )

    async def registerForPublish(self, groupName: str) -> ChannelGroup:
        if not self._telemetry:
            raise RuntimeError("Telemetry is not configured.")

        groupCfg = self.config.lookupGroupByName(groupName)

        if not groupCfg:
            raise ValueError(f'Unknown group name "{groupName}".')

        return await self._telemetry.registerForPublish(groupCfg)

    async def subscribe(
        self, groupName: str, timeout_s: float = None
    ) -> tuple[ChannelGroup, bool]:
        """Subscribes to the telemetry data from the specified group.

        Groups are named collections of channels that are published together. Once subscribed, the channels within the group
        will be updated and values can be read from channel objects directly or all at once directly from the group.

        :param groupName: The name of the group to subscribe to.
        :type groupName: str
        :param timeout_s: How much time to wait for data to arrive after subscribing, defaults to None
        :type timeout_s: float
        :raises ValueError: The specified group name was not found in the system configuration.
        :raises RuntimeError: The configuration for the node this Python app is running as was not configured for networking.
        :return: The group that has been subscribed to.
        :rtype: tuple[ChannelGroup, bool]
        """

        if self._telemetry:
            groupCfg = self.config.lookupGroupByName(groupName)

            if not groupCfg:
                raise ValueError(f'Unknown group name "{groupName}".')

            return await self._telemetry.subscribe(groupCfg, timeout_s)

        raise RuntimeError(
            "Volatus is not configured for networking and the telemetry component is not available."
        )

    def publish(self, group: ChannelGroup):
        """Publishes current values for a group as telemetry.

        :param group: The channel group to publish.
        :type group: ChannelGroup
        """
        if not self._telemetry:
            raise RuntimeError(
                "Volatus has not been configured with Telemetry capabilities."
            )

        self._telemetry.publish(group)

    def unsubscribe(self, group: ChannelGroup):
        """Not implemented yet.

        :param group: The group that was subscribed to.
        :type group: ChannelGroup
        """
        pass

    def createReportEventMsg(
        self, targetName: str, level: EventLevel, context: str, message: str = ""
    ) -> VCommand:
        event = Event()
        event.context = context
        event.message = message
        event.level = level

        msg = Events()
        msg.events.append(event)

        return VCommand(
            targetName,
            "v:Events",
            msg.SerializeToString(),
            self._tcp.sendMsg,
        )

    def createReportErrorMsg(
        self,
        targetName: str,
        errCode: int,
        errMsg: str,
        context: str,
        message: str = "",
    ) -> VCommand:
        event = Event()
        event.context = context
        event.message = message
        event.level = EventLevel.EVENTLEVEL_ERROR
        event.error.code = errCode
        event.error.status = True
        event.error.source = errMsg

        msg = Events()
        msg.events.append(event)

        return VCommand(
            targetName,
            "v:Events",
            msg.SerializeToString(),
            self._tcp.sendMsg,
        )

    def reportEvent(
        self, targetName: str, level: EventLevel, context: str, message: str = ""
    ):
        event = Event()
        event.context = context
        event.message = message
        event.level = level

        msg = Events()
        msg.events.append(event)

        self._tcp.sendMsg(targetName, "v:Events", msg.SerializeToString())

    def reportError(
        self,
        targetName: str,
        errCode: int,
        errMsg: str,
        context: str,
        message: str = "",
    ):
        event = Event()
        event.context = context
        event.message = message
        event.level = EventLevel.EVENTLEVEL_ERROR
        event.error.code = errCode
        event.error.status = True
        event.error.source = errMsg

        msg = Events()
        msg.events.append(event)

        self._tcp.sendMsg(targetName, "v:Events", msg.SerializeToString())
