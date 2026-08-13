"""
Demonstrates registering functions with Volatus to perform calculations.
This requires a PyCalcs task type configured in the vjson to provide the derived
channels to publish to.

The calculations in this example are simple and could easily be handled as
"Expression" configuration for the channels in the vjson, but this pattern
enables use-cases such as importing refprop/coolprop and other libraries
to perform more involved calculations.
"""

from volatus.volatus import Volatus, ModuleIdentity
from volatus import calcs

def simple_sum_calc(inputs: dict[str, float]) -> float:
    return inputs["sum_a"] + inputs["sum_b"]

avg = calcs.RunningAvg(100)
def avg_calc(inputs: dict[str, float]) -> float:
    return avg.add(inputs["avg_input"])

@Volatus.ini_app
async def app(v: Volatus):
    # Since telemetered channels need to be defined ahead of time, CalcsTest module is already defined in config
    # Known task types in config are launched automatically so we need to retrieve it by the name we expect.
    c = await v.lookup_id_timeout(ModuleIdentity("CalcsTest"), calcs.CalcsModule)
    if not c:
        print(f"Calcs module lookup failed, check module name or increase timeout. Aborting.")
        return

    # Can now register calcs methods that use specified input channel values and the specified method
    # to generate updated values for the output channels.
    # These are simple calcs but this pattern enables utilizing external libs such as refprop/coolprop
    await c.add_calc_method("sum_calc", simple_sum_calc, ["sum_a", "sum_b"])
    await c.add_calc_method("avg_calc", avg_calc, ["avg_input"])

    await v.wait_terminate()