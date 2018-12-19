import unittest

from vizier.config.worker import WorkerConfig
from vizier.engine.packages.mimir.base import PACKAGE_MIMIR
from vizier.engine.packages.plot.base import PACKAGE_PLOT
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL

CONFIG_FILE = './.files/config.worker.yaml'


class TestConfig(unittest.TestCase):

    def test_default_config(self):
        """Test the default configuration settings.
        """
        config = WorkerConfig(configuration_file=CONFIG_FILE)


if __name__ == '__main__':
    unittest.main()
