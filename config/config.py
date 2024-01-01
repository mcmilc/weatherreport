"""Global configurations."""
import os
from weatherreport.utilities.filesystem_utils import pjoin
from weatherreport.utilities.filesystem_utils import pexists

WR_TMPDIR = "WEATHERREPORT_TEMPDIR"
weather_report_root = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def setup_temp_datastore(var_name):
    """Setup directory for temporary data storage."""
    temp_dir = pjoin(weather_report_root, ".tmp")
    if not pexists(temp_dir):
        os.mkdir(temp_dir)
    if var_name not in os.environ.keys():
        os.environ[var_name] = temp_dir


setup_temp_datastore(WR_TMPDIR)
