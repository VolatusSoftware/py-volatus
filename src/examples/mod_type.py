import volatus.calcs
from volatus.volatus import Volatus

@Volatus.main
async def main():
    async with Volatus.from_ini() as v:
        await v.wait_terminate()