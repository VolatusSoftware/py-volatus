"""
This is the minimal boilerplate needed to run a Python based Volatus application.
"""

from volatus.volatus import Volatus

# This import makes the CalcsModule (PyCalcs type in config) available and
# the import automatically registers the module with Volatus so it can be
# run if configured in the vjson.
from volatus import calcs

# The ini_app decorator, by default, expects a "volatus.ini" file to exist in the CWD
# Which specifies which node to run as and where to find the vjson configuration file.
#
# The decorator can be used to specify the application version or a different name/location
# for the ini file that defines the node and vjson configuration details.
@Volatus.ini_app
async def app(v: Volatus):

    # Assuming 100% of the app is loaded from the configuration, all that needs to be
    # done is wait for the application to terminate.
    await v.wait_terminate()