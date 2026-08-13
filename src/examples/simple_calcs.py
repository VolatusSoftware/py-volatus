from volatus.volatus import Volatus
from volatus import calcs

def simple_sum_calc(inputs: dict[str, float]) -> dict[str, float]:
    return {
        "sum_calc": inputs["sum_a"] + inputs["sum_b"],
    }

avg = calcs.RunningAvg(100)
def avg_calc(inputs: dict[str, float]) -> dict[str, float]:
    return {
        "avg_calc": avg.add(inputs["avg_input"])
    }

@Volatus.main
async def main():
    async with Volatus.from_ini() as v:
        # PyCalcs task in the vjson contains the published telemetry group
        await v.init_calcs("PyCalcs")

        await v.add_calc(["sum_a", "sum_b"], ["sum_calc"], simple_sum_calc)
        await v.add_calc(["avg_input"], ["avg_calc"], avg_calc)

        await v.wait_terminate()