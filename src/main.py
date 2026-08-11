from volatus.config import Cfg, VL_Type
from volatus.volatus import Volatus, EventLevel, LogState, TcpPayload, TCPMessaging
from volatus.telemetry import ChannelGroup, ChannelValue
from volatus.proto.cmd_digital_pb2 import *
from pydantic import BaseModel, ValidationError
from pathlib import Path
import numpy as np

import asyncio

# provide a "cleaner" path format that doesn't trip up on escape sequences.
# this is the same format used for paths in vjson files.
INI_PATH = Path('volatus.ini')

# meaningless to Volatus but is reported on TCP connection for visibility in GUIs
APP_VERSION = "0.1.0"

class PythonTestModel (BaseModel):
    CustomValue: float
    StringVal: str

# Handles dispatched messages for the "cmd_digital" message.
# 'msg' is not used here but could be used for replying, sending another message, etc.
async def cmdTest(payload: TcpPayload, msg: TCPMessaging):
    #registered for cmd_digital messages, parse embedded msg format
    cmd = CmdDigital.FromString(payload.payload)

    print(f"Setting {cmd.channel} to {cmd.value}")

def simple_sum_calc(inputs: dict[str, float]) -> dict[str, float]:
    return {
        "sum_calc": inputs["sum_a"] + inputs["sum_b"],
    }

avg_n: float = 0.0
avg_i = 0
avg_nums = np.zeros(100, dtype=np.float64)

def avg_calc(inputs: dict[str, float]) -> dict[str, float]:
    avg_nums[avg_i] = inputs["avg_input"]
    avg_i = avg_i + 1 if avg_i < 99 else 0

    if avg_n < 100:
        avg_n += 1

    return {
        "avg_calc": avg_nums.sum() / avg_n
    }


async def main():
    # create the top level Volatus object. The Volatus class handles config loading
    # and initializing the components as configured. With the Context Manager support
    # the initialized volatus object is automatically shutdown at the end of the with block.
    async with Volatus.from_ini(INI_PATH, APP_VERSION, 10) as v:

        v.registerMessageHandler("cmd_digital", "PythonTest", cmdTest)

        v.add_calc(["sum_a", "sum_b"], ["sum_calc"], simple_sum_calc)
        v.add_calc(["avg_input"], ["avg_calc"], avg_calc)

        tasks = Cfg.vlFindType(v.config, VL_Type.VL_Task, False)
        print(f"Found {len(tasks)} tasks.")

        taskCfg = v.config.lookupTaskByName('PythonTest', 'PyScript')
        val = taskCfg.lookupChildByName('CustomValue')
        print(f"CustomValue: {val.value()}")

        mCfg = taskCfg.loadModel(PythonTestModel)
        print(f"StringVal from model: {mCfg.StringVal}")

        gAI: ChannelGroup
        hasData: bool

        # subscribe to a known group we're interested in reading published data from
        gAI, hasData = await v.subscribe('TestAI', 2)

        vals, _ = gAI.allValues()
        print(vals)

        if hasData:
            print("Data valid within timeout.")
        else:
            print("No data received yet.")

        _, hasLogData = await v.subscribe('Logging_Status', 5)

        if hasLogData:
            print("Subscribed to logging status")
        else:
            print("No data within timeout for logging status")

        # get a single channel to read live values from
        ch0: ChannelValue = gAI.chanByName('AI00')

        # register published python group that is available from config
        pyGroup = await v.registerForPublish("PythonData")
        
        # use a local list for ease of group value updates
        pyVals = [3.1, 3.2, 3.3]

        # put the initial values into the group which also automatically applies the current timestamp
        pyGroup.updateValues(pyVals)

        # perform an initial publish to get the values out of a stale state
        v.publish(pyGroup)

        v.reportEvent('Events', EventLevel.EVENTLEVEL_INFO, 'Test Python', 'Starting sequencing')

        # creates and immediately sends a start log command
        v.createStartLogCommand('Logging', 'testy', 'python').send()
        logging = await v.waitForLogState(LogState.Logging)

        if not logging:
            print('Log unable to start, aborting.')
            exit()

        # turn digital output on, for scaled values (such as inverted NO valves) this will be before scaling
        # typically meaning valves are always True = Open, False = Closed
        # the create___Command methods return a VCommand object with a send() that can be called right away or sent later
        v.createDigitalCommand('DO00', True).send()

        # loop ~10Hz displaying current value for the channel
        # updates first PythonData value to publish as telemetry
        for i in range(40):
            pyVals[0] += 1.0
            pyGroup.updateValues(pyVals)
            v.publish(pyGroup)

            print(ch0.value)
            await asyncio.sleep(0.1)

        # turn digital output back off
        v.createDigitalCommand('DO00', False).send()

        v.reportEvent('Events', EventLevel.EVENTLEVEL_INFO, 'Test Python', 'Sequence complete')

        v.createStopLogCommand('Logging', 'Stopping').send()

        # ensure stop log command has been handled before allowing app to close.
        # this helps make sure the command has had time to make it through the async
        # tasks and actually get out to the targets."?:"
        await v.waitForLogState(LogState.Idle)

if __name__ == '__main__':
    asyncio.run(main())
