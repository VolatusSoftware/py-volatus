from volatus.config import Cfg
from volatus.volatus import Volatus, EventLevel, LogState
from volatus.telemetry import ChannelGroup, ChannelValue

import asyncio

# provide a "cleaner" path format that doesn't trip up on escape sequences.
# this is the same format used for paths in vjson files.
cfgPath = Cfg.normalizePath('c:/dev/lv20ce/relink/lv-volatus/VolatusScratch/daqtest.vjson')


async def main():
    # create the top level Volatus object. The Volatus class handles config loading
    # and initializing the components as configured. With the Context Manager support
    # the initialized volatus object is automatically shutdown at the end of the with block.
    async with Volatus(cfgPath, 'TestSystem', 'TestCluster', 'PyScript') as v:

        gAI: ChannelGroup
        hasData: bool

        # subscribe to a known group we're interested in reading published data from
        gAI, hasData = await v.subscribe('TestAI', 2)

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

        # register published python group and initialize with starting data
        pyGroup = await v.registerForPublish("PythonData")
        
        pyVals = [3.1, 3.2, 3.3]
        pyGroup.updateValues(pyVals)
        v.publish(pyGroup)

        v.reportEvent('Events', EventLevel.EVENTLEVEL_INFO, 'Test Python', 'Starting sequencing')

        v.createStartLogCommand('Logging', 'testy', 'python').send()

        logging = await v.waitForLogState(LogState.Logging)

        if not logging:
            print('Log unable to start.')
            #exit()

        # turn digital output on, for scaled values (such as inverted NO valves) this will be before scaling
        # typically meaning valves are always True = Open, False = Closed
        # the create___Command methods return a VCommand object with a send() that can be called right away or sent later
        v.createDigitalCommand('DO00', True).send()

        # loop ~10Hz displaying current value for the channel
        # run long enough to get some discovery packets out
        for i in range(20):
            pyVals[0] += 1.0
            pyGroup.updateValues(pyVals)
            v.publish(pyGroup)

            print(ch0.value)
            await asyncio.sleep(0.1)

        # turn digital output back off
        v.createDigitalCommand('DO00', False).send()

        v.reportEvent('Events', EventLevel.EVENTLEVEL_INFO, 'Test Python', 'Sequence complete')

        v.createStopLogCommand('Logging', 'Stopping').send()

        #ensure stop log command has been handled before allowing app to close.
        await v.waitForLogState(LogState.Idle)

if __name__ == '__main__':
    asyncio.run(main())
