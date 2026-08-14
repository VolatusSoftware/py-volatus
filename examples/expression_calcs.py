"""
Demonstrates registering expressions with Volatus to perform calculations.
This requires a PyCalcs task type configured in the vjson to provide the derived
channels to publish to.

This pattern separates the expression from the vjson configuration. This approach
can be useful when it is desirable to be able to update calculations without
needing to deploy an updated vjson around a test system, which would be the case
when using the "Expression" field for channels in the vjson.
"""

from volatus.volatus import Volatus, ModuleIdentity
from volatus import calcs

@Volatus.ini_app
async def app(v: Volatus):
    # Since telemetered channels need to be defined ahead of time, CalcsTest module is already defined in config
    # Known task types in config are launched automatically so we need to retrieve it by the name we expect.
    c = await v.lookup_id_timeout(ModuleIdentity("CalcsTest"), calcs.CalcsModule)
    if not c:
        print(f"Calcs module lookup failed, check module name or increase timeout. Aborting.")
        return

    # Can now register expressions for the derived channels. This is the same exact format
    # that would be used with the "Expression" field for the channel in the vjson.
    # Programmatically configured expressions can be mixed with channels that have their
    # expression configured in the vjson.
    await c.add_calc_expression("sum_calc", "[sum_a] + [sum_b]")
    await c.add_calc_expression("avg_calc", "run_avg([avg_input], 100)")

    await v.wait_terminate()