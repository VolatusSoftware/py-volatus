from pathlib import Path
from enum import Enum
import json
import queue
import os
import hashlib
from typing import Optional, Self

from pydantic import BaseModel, ValidationError


class VL_Type(Enum):
    UNKNOWN = 0
    VL_System = 1
    VL_Cluster = 2
    VL_Node = 3
    VL_Task = 4
    VL_Group = 5
    VL_Channel = 6
    VL_Task_List = 7
    VL_Scaling = 8
    VL_Sensor = 9
    VL_Sensor_List = 10
    VL_Scale = 11

    def __str__(self):
        return f"{self.name}"

    def fromStr(s: str) -> "VL_Type":
        if not s:
            return None

        try:
            e = VL_Type[s]
            return e
        except KeyError:
            return VL_Type.UNKNOWN


class VL_Meta(Enum):
    UNKNOWN = 0
    VL_Type = 1
    VL_Task_Type = 2
    VL_Group_Type = 3
    VL_Config_Version = 4

    def __str__(self):
        return f"{self.name}"


class EndpointConfig:
    def __init__(self, address: str, port: int):
        self.address: str = address
        self.port: int = port

    def __eq__(self, other) -> bool:
        if isinstance(other, EndpointConfig):
            return self.address == other.address and self.port == other.port
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return f"{self.address}:{self.port}"

    def tuple(self) -> tuple[str, int]:
        return (self.address, self.port)


class TelemetryRouting(Enum):
    UNKNOWN = 0
    Multicast = 1
    Unicast = 2

    def __str__(self):
        return f"{self.name}"

    def fromStr(s: str) -> "TelemetryRouting":
        if not s:
            return None

        try:
            e = TelemetryRouting[s]
            return e
        except KeyError:
            return TelemetryRouting.UNKNOWN


class TelemetryConfig:
    def __init__(self, routing: TelemetryRouting, endpt: EndpointConfig):
        self.routing: TelemetryRouting = routing
        self.endpt: EndpointConfig = endpt

    def __eq__(self, other) -> bool:
        if isinstance(other, TelemetryConfig):
            return self.routing == other.routing and self.endpt == other.endpt
        else:
            return NotImplemented

    def __hash__(self) -> int:
        return hash(str(self))

    def __str__(self) -> str:
        return f"{self.routing}@{self.endpt}"


class ConfigElementBase:
    def __init__(self, name: str, val: int | bool | str | float):
        self.val = val
        self.name = name
        self._hasChildren = isinstance(val, dict)

    def lookupChildByName(self, childName: str) -> 'ConfigElementBase':
        if self._hasChildren:
            child = self.val.get(childName)

            if not child:
                return None

            return ConfigElementBase(childName, child)

        return None
    
    def lookupMetaValue(self, metaName: str):
        if isinstance(self.val, dict):
            return Cfg.readMetaValue(self.val, metaName)
        
        return None
    
    def value(self):
        if isinstance(self.val, dict):
            return self.val.get("Value")
        
        return self.val
    
    def loadModel[T: BaseModel](self, model_cls: type[T], validate: bool = True) -> T:
        if validate:
            return model_cls(**self.val)
        else:
            return model_cls.model_construct(**self.val)
        
    def children(self) -> list[Self]:
        c: list[Self] = []
        c_dict = Cfg.childrenOf(self)
        for name, child in c_dict.items():
            c.append(ConfigElementBase(name, child))

        return c

class ChannelConfig(ConfigElementBase):
    def __init__(
        self,
        name: str,
        groupName: str,
        taskName: str,
        nodeName: str,
        clusterName: str,
        cfgObj: dict,
        defaultValue: str | float = None,
        resource: str = "",
    ):

        self.name = name
        self.defaultValue = defaultValue
        self.resource = resource
        self.groupName = groupName
        self.taskName = taskName
        self.nodeName = nodeName
        self._clusterName = clusterName

        super().__init__(name, cfgObj)

    def clusterName(self) -> str:
        return self._clusterName

    def setClusterName(self, clusterName: str):
        self._clusterName = clusterName


class GroupConfig(ConfigElementBase):
    def __init__(
        self,
        name: str,
        taskName: str,
        nodeName: str,
        clusterName: str,
        cfgObj: dict,
        channels: list[ChannelConfig] = [],
        publishConfig: EndpointConfig = None,
    ):
        self.name = name
        self.publishConfig = publishConfig
        self.channels: dict[str, ChannelConfig] = dict()
        self.taskName = taskName
        self.nodeName = nodeName
        self.clusterName = clusterName

        super().__init__(name, cfgObj)

        for channel in channels:
            self.channels[channel.name] = channel

    def isPublished(self) -> bool:
        return self.publishConfig is not None

    def addChannel(self, channel: ChannelConfig):
        self.channels[channel.name] = channel

    def setClusterName(self, clusterName: str):
        self.clusterName = clusterName
        for channel in self.channels.values():
            channel.setClusterName(clusterName)

    def lookupChannelByName(self, channelName: str) -> ChannelConfig:
        return self.channels.get(channelName)


class TaskConfig(ConfigElementBase):
    def __init__(
        self,
        name: str,
        type: str,
        nodeName: str,
        clusterName: str,
        cfgObj: dict,
        groups: list[GroupConfig] = [],
    ):
        self.name = name
        self.type = type
        self.groups: dict[str, GroupConfig] = dict()
        self.nodeName = nodeName
        self.clusterName = clusterName

        super().__init__(name, cfgObj)

        for group in groups:
            self.groups[group.name] = group

    def setClusterName(self, clusterName: str):
        self.clusterName = clusterName
        for group in self.groups.values():
            group.setClusterName(clusterName)

    def addGroup(self, group: GroupConfig):
        self.groups[group.name] = group

    def lookupGroupByName(self, groupName: str) -> GroupConfig | None:
        return self.groups.get(groupName)


class TCPConfig:
    def __init__(self, address: str, port: int, server: bool):
        self.address = address
        self.port = port
        self.server = server


class NodeNetworkConfig:
    def __init__(
        self,
        tcpConfig: TCPConfig,
        httpPort: int = None,
        announceInterval: int = None,
        bindAddress: str = "0.0.0.0",
    ):
        self.tcp = tcpConfig
        self.httpPort = httpPort
        self.announceInterval = announceInterval
        self.bindAddress = bindAddress


class NodeConfig(ConfigElementBase):
    def __init__(
        self,
        name: str,
        id: int,
        clusterName: str,
        cfgObj: dict,
        eventLogFolder: Path = None,
        network: NodeNetworkConfig = None,
        targetGroups: list[str] = [],
        tasks: list[TaskConfig] = [],
    ):
        self.name = name
        self.id = id
        self.logFolder = eventLogFolder
        self.network = network
        self.targetGroups = targetGroups
        self.tasks: dict[str, TaskConfig] = dict()
        self.clusterName = clusterName

        super().__init__(name, cfgObj)

        for task in tasks:
            self.tasks[task.name] = task

    def tcpConfig(self) -> TCPConfig | None:
        if self.network:
            return self.network.tcp()

        return None

    def addTask(self, task: TaskConfig):
        self.tasks[task.name] = task

    def setClusterName(self, clusterName: str):
        self.clusterName = clusterName
        for taskObj in self.tasks.values():
            taskObj.setClusterName(clusterName)

    def lookupTaskByName(self, taskName: str) -> TaskConfig | None:
        return self.tasks.get(taskName)


class ClusterConfig(ConfigElementBase):
    def __init__(
        self,
        name: str,
        cfgObj: dict,
        telemetryConfig: TelemetryConfig = None,
        targetGroups: dict[str, int] = None,
        nodes: list[NodeConfig] = None,
    ):
        self.name = name
        self.telemetry = telemetryConfig
        self.targetGroups = targetGroups
        self.nodes: dict[str, NodeConfig] = dict()

        super().__init__(name, cfgObj)

        if nodes:
            for node in nodes:
                self.nodes[node.name] = node

    def lookupTargetGroupId(self, targetName: str) -> int | None:
        if self.targetGroups:
            return self.targetGroups.get(targetName)

        return None

    def lookupNodeByName(self, nodeName: str) -> NodeConfig | None:
        return self.nodes.get(nodeName)

    def addNode(self, node: NodeConfig):
        self.nodes[node.name] = node


class SystemConfig(ConfigElementBase):
    def __init__(self, name: str, cfgObj: dict, clusters: list[ClusterConfig] = []):
        self.name = name
        self.clusters: dict[str, ClusterConfig] = dict()

        super().__init__(name, cfgObj)

        for cluster in clusters:
            self.clusters[cluster.name] = cluster

    def lookupClusterByName(self, clusterName: str) -> ClusterConfig | None:
        return self.clusters.get(clusterName)

    def addCluster(self, cluster: ClusterConfig):
        self.clusters[cluster.name] = cluster


class VersionBump(Enum):
    NONE = 0
    FIX = 1
    MINOR = 2
    MAJOR = 3


class VolatusVersion:
    def __init__(
        self, major: int, minor: int, fix: int, build: int = 0, prerelease: str = ""
    ):
        self.major = major
        self.minor = minor
        self.fix = fix
        self.build = build
        self.prerelease = prerelease

    def __str__(self) -> str:
        ver = f"{self.major}.{self.minor}.{self.fix}"
        if self.prerelease:
            ver += f"-{self.prerelease}"

        if self.build > 0:
            ver += f"+{self.build}"

        return ver

    def fromString(versionStr: str) -> "VolatusVersion":
        parts = versionStr.split(".")
        major = int(parts[0])
        minor = int(parts[1])
        build = 0
        prerelease = ""

        buildParts = parts[2].split("+")
        if len(buildParts) > 1:
            build = int(buildParts[1])

        preParts = buildParts[0].split("-")
        if len(preParts) > 1:
            prerelease = preParts[1]

        fix = int(preParts[0])

        return VolatusVersion(major, minor, fix, build, prerelease)

    def bump(self, bumpType: VersionBump):
        match bumpType:
            case VersionBump.NONE:
                if self.build:
                    self.build += 1
                else:
                    self.build = 1

            case VersionBump.FIX:
                self.fix += 1

                if self.build:
                    self.build += 1
                else:
                    self.build = 1

            case VersionBump.MINOR:
                self.minor += 1
                self.fix = 0

                if self.build:
                    self.build += 1
                else:
                    self.build = 1

            case VersionBump.MAJOR:
                self.major += 1
                self.minor = 0
                self.fix = 0

                if self.build:
                    self.build += 1
                else:
                    self.build = 1


class ClusterLookup:
    def __init__(self, clusterName: str):
        self.clusterName = clusterName


class NodeLookup(ClusterLookup):
    def __init__(self, nodeName: str, clusterName: str):
        self.nodeName = nodeName
        super(NodeLookup, self).__init__(clusterName)


class TaskLookup(NodeLookup):
    def __init__(self, taskName: str, nodeName: str, clusterName: str):
        self.taskName = taskName
        super(TaskLookup, self).__init__(nodeName, clusterName)


class GroupLookup(TaskLookup):
    def __init__(self, groupName: str, taskName: str, nodeName: str, clusterName: str):
        self.groupName = groupName
        super(GroupLookup, self).__init__(taskName, nodeName, clusterName)


class ChannelLookup(GroupLookup):
    def __init__(
        self,
        channelName: str,
        groupName: str,
        taskName: str,
        nodeName: str,
        clusterName: str,
    ):
        self.channelName = channelName
        super(ChannelLookup, self).__init__(groupName, taskName, nodeName, clusterName)


class VolatusConfig(ConfigElementBase):
    def __init__(
        self,
        cfgObj: dict,
        version: VolatusVersion = None,
        hash: str = None,
        system: SystemConfig = None,
    ):
        self.version = version
        self.hash = hash
        self.system = system
        self.groups: dict[str, TaskLookup] = dict()
        self.channels: dict[str, GroupLookup] = dict()

        super().__init__('Volatus', cfgObj)

        self.refreshLookups()

    def _addSystemDicts(self, system: SystemConfig):
        for cluster in system.clusters.values():
            self._addClusterDicts(cluster)

    def _addClusterDicts(self, cluster: ClusterConfig):
        for node in cluster.nodes.values():
            self._addNodeDicts(node)

    def _addNodeDicts(self, node: NodeConfig):
        for task in node.tasks.values():
            self._addTaskDicts(task)

    def _addTaskDicts(self, task: TaskConfig):
        for group in task.groups.values():
            self._addGroupDicts(group)

    def _addGroupDicts(self, group: GroupConfig):
        self.groups[group.name] = TaskLookup(
            group.taskName, group.nodeName, group.clusterName
        )
        channels = group.channels
        for channel in channels.values():
            self.channels[channel.name] = GroupLookup(
                group.name, group.taskName, group.nodeName, group.clusterName
            )

    def refreshLookups(self):
        self.groups = dict()
        self.channels = dict()

        if self.system:
            self._addSystemDicts(self.system)

    def lookupCluster(self, cl: ClusterLookup) -> ClusterConfig | None:
        return self.lookupClusterByName(cl.clusterName)

    def lookupClusterByName(self, clusterName: str) -> ClusterConfig | None:
        if self.system:
            return self.system.lookupClusterByName(clusterName)

        return None

    def lookupNode(self, nl: NodeLookup) -> NodeConfig | None:
        return self.lookupNodeByName(nl.nodeName, nl.clusterName)

    def lookupNodeByName(
        self, nodeName: str, clusterName: str = None
    ) -> NodeConfig | None:
        if clusterName:
            cluster = self.lookupClusterByName(clusterName)
            if cluster:
                return cluster.lookupNodeByName(nodeName)
        elif self.system:
            clusters = self.system.clusters
            for clusterName, cluster in clusters.items():
                node = cluster.lookupNodeByName(nodeName)
                if node:
                    return node

        return None

    def lookupTask(self, tl: TaskLookup) -> TaskConfig | None:
        return self.lookupTaskByName(tl.taskName, tl.nodeName, tl.clusterName)

    def lookupTaskByName(
        self, taskName: str, nodeName: str, clusterName: str = None
    ) -> TaskConfig:
        node = self.lookupNodeByName(nodeName, clusterName)
        if node:
            return node.lookupTaskByName(taskName)

        return None

    def lookupGroup(self, gl: GroupLookup) -> GroupConfig | None:
        task = self.lookupTask(gl)
        if task:
            return task.lookupGroupByName(gl.groupName)

        return None

    def lookupGroupByName(self, groupName: str) -> GroupConfig | None:
        tl = self.groups.get(groupName)
        if tl:
            task = self.lookupTask(tl)
            if task:
                return task.lookupGroupByName(groupName)

        return None

    def lookupChannel(self, cl: ChannelLookup) -> ChannelConfig | None:
        group = self.lookupGroup(cl)
        if group:
            return group.lookupChannelByName(cl.channelName)

        return None

    def group_name_for_channel(self, chan_name: str) -> str:
        gl = self.channels.get(chan_name)
        if gl:
            return gl.groupName
        else:
            return ""

    def lookupChannelByName(self, channelName: str) -> ChannelConfig | None:
        gl = self.channels.get(channelName)
        if gl:
            group = self.lookupGroup(gl)
            if group:
                return group.lookupChannelByName(channelName)

        return None


def cfgObj(obj: dict | ConfigElementBase) -> dict:
    if isinstance(obj, ConfigElementBase):
        return obj.val

    return obj


class Cfg:
    def normalizePath(pathStr: str) -> Path:
        pathArr = pathStr.split("/")
        path: Path = None

        for segment in pathArr:
            if not path:
                path = Path(segment + os.sep)
            else:
                path = path.joinpath(segment)

        return path

    def childrenOf(obj: dict | ConfigElementBase) -> dict[str, dict]:
        obj = cfgObj(obj)
        return {k: v for k, v in obj.items() if k != "Meta"}

    def readMetaValue(obj: dict | ConfigElementBase, name: str) -> str | None:
        obj = cfgObj(obj)
        meta: dict = obj.get("Meta")
        if meta:
            return meta.get(name)

        return None

    def writeMetaValue(obj: dict | ConfigElementBase, name: str, value: str):
        obj = cfgObj(obj)
        if "Meta" not in obj:
            obj["Meta"] = {name: value}
        else:
            obj["Meta"][name] = value

    def vlReadMeta(obj: dict | ConfigElementBase, meta: VL_Meta) -> str | None:
        obj = cfgObj(obj)
        m: dict = obj.get("Meta")
        if m:
            return m.get(str(meta))

        return None

    def vlWriteMeta(obj: dict | ConfigElementBase, meta: VL_Meta, value: str):
        Cfg.writeMetaValue(obj, str(meta), value)

    def vlSetType(obj: dict | ConfigElementBase, type: VL_Type):
        Cfg.vlWriteMeta(obj, VL_Meta.VL_Type, str(type))

    def vlTypeOf(obj: dict | ConfigElementBase) -> VL_Type | None:
        return VL_Type.fromStr(Cfg.vlReadMeta(obj, VL_Meta.VL_Type))

    def vlFindType(
        obj: dict | ConfigElementBase, type: VL_Type, recursePastMatch: bool = True
    ) -> dict[str, dict]:
        obj = cfgObj(obj)
        matches = dict()

        q: queue.Queue[tuple[str, dict]] = queue.Queue()
        q.put(("", obj))

        while not q.empty():
            name, o = q.get()
            recurse = True
            if Cfg.vlTypeOf(o) == type and name != "":
                matches[name] = o
                if not recursePastMatch:
                    recurse = False

            if recurse:
                children: dict = Cfg.childrenOf(o)
                for name, child in children.items():
                    if isinstance(child, dict):
                        q.put((name, child))

        return matches


class ConfigLoader:
    def load(path: Path) -> VolatusConfig:
        hash = ""

        # hash needs to be calculated with binary mode
        with open(path, "rb") as file:
            hash = hashlib.file_digest(file, "sha256").hexdigest().upper()

        with open(path, "r", encoding="utf-8") as file:
            cfg = json.load(file)
            version = VolatusVersion.fromString(
                cfg["Volatus"]["Meta"]["VL_Config_Version"]
            )

            # should only be a single non-Meta child in the volatus object
            children = Cfg.childrenOf(cfg["Volatus"])
            systemName, sysObj = next(iter(children.items()))

            system = SystemConfig(systemName, sysObj)
            vCfg = VolatusConfig(cfg, version, hash, system)

            ConfigLoader.loadSystem(system, sysObj)

            vCfg.refreshLookups()
            return vCfg

    def save(path: Path, config: VolatusConfig):
        pass

    def loadGroup(task: TaskConfig, groupName: str, groupObj: dict):
        groupType = Cfg.vlReadMeta(groupObj, VL_Meta.VL_Group_Type)

        pubCfg: EndpointConfig = None
        pubObj = groupObj.get("Multicast")

        if not pubObj:
            # backwards compatibility for older name
            pubObj = groupObj.get("Publish")

        if pubObj:
            pubCfg = EndpointConfig(pubObj["Address"], pubObj["Port"])

        group = GroupConfig(
            groupName,
            task.name,
            task.nodeName,
            task.clusterName,
            groupObj,
            publishConfig=pubCfg,
        )
        task.addGroup(group)

        chansObj: dict[str, dict] = Cfg.vlFindType(groupObj, VL_Type.VL_Channel)
        for channelName, chanObj in chansObj.items():
            resource = chanObj.get("Resource")
            if not resource:
                resource = ""

            channel = ChannelConfig(
                channelName,
                groupName,
                task.name,
                task.nodeName,
                task.clusterName,
                chanObj,
                chanObj.get("Value"),
                resource,
            )

            group.addChannel(channel)

    def loadTask(node: NodeConfig, taskName: str, taskObj: dict):
        taskType = Cfg.vlReadMeta(taskObj, VL_Meta.VL_Task_Type)

        task = TaskConfig(taskName, taskType, node.name, node.clusterName, taskObj)
        node.addTask(task)

        groups: dict[str, dict] = Cfg.vlFindType(taskObj, VL_Type.VL_Group, False)
        for groupName, groupObj in groups.items():
            ConfigLoader.loadGroup(task, groupName, groupObj)

    def loadNode(cluster: ClusterConfig, nodeName: str, nodeObj: dict):
        nodeId = nodeObj["Node_ID"]
        # debug = nodeObj['DebugGUIs']
        logFolder = Cfg.normalizePath(nodeObj["Events"]["LogFolder"])

        netCfg: NodeNetworkConfig = None
        net: dict = nodeObj.get("Network")
        if net:
            tcpCfg: TCPConfig = None
            tcp = net.get("TCP")
            if tcp:
                tcpCfg = TCPConfig(tcp["Address"], tcp["Port"], tcp["Server"])

            httpPort = net.get("HTTP_Port")
            announceInterval = net.get("Announce_Interval")
            bindAddress = net.get("Bind_Address")

            if not bindAddress:
                bindAddress = "0.0.0.0"

            netCfg = NodeNetworkConfig(tcpCfg, httpPort, announceInterval, bindAddress)

        targetGroups = []
        groups = nodeObj.get("Groups")
        if groups:
            for group in groups:
                targetGroups.append(group)

        node = NodeConfig(
            nodeName, nodeId, cluster.name, nodeObj, logFolder, netCfg, targetGroups
        )
        cluster.addNode(node)

        tasksObj = nodeObj.get("Tasks")
        if tasksObj:
            tasks: dict[str, dict] = Cfg.childrenOf(tasksObj)
            for taskName, taskObj in tasks.items():
                ConfigLoader.loadTask(node, taskName, taskObj)

    def loadCluster(system: SystemConfig, clusterName: str, clusterObj: dict):
        # Discovery endpoint config is the multicast endpoint nodes advertise on for dynamic mapping
        discObj = clusterObj.get("Discovery")
        disc = None
        if discObj:
            disc = EndpointConfig(discObj["Address"], discObj["Port"])

        telemObj = clusterObj.get("Telemetry")
        telem = None
        if telemObj:
            routing: TelemetryRouting = TelemetryRouting.fromStr(telemObj["Routing"])
            endptObj = telemObj.get("Endpoint")
            endpt: EndpointConfig = EndpointConfig(
                endptObj["Address"], endptObj["Port"]
            )
            telem = TelemetryConfig(routing, endpt)

        groupsObj = clusterObj.get("Groups")

        cluster = ClusterConfig(clusterName, clusterObj, telem, groupsObj)
        system.addCluster(cluster)

        nodesObj = clusterObj.get("Nodes")
        nodes: dict[str, dict] = Cfg.childrenOf(nodesObj)
        for nodeName, nodeObj in nodes.items():
            ConfigLoader.loadNode(cluster, nodeName, nodeObj)

    def loadSystem(system: SystemConfig, sysObj: dict):

        # ignoring sensor list for now, not needed by python automation scripts

        # iterate over and create clusters
        sysChildren = Cfg.childrenOf(sysObj)
        clustersObj = sysChildren["Clusters"]
        clusters = Cfg.childrenOf(clustersObj)

        for clusterName, clusterObj in clusters.items():
            ConfigLoader.loadCluster(system, clusterName, clusterObj)
