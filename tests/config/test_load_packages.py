import os
import unittest

from vizier.config.app import AppConfig
from vizier.engine.packages.load import load_packages
from vizier.engine.packages.plot.base import PACKAGE_PLOT
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL

import vizier.config.app as env


PACKAGES_DIR = './.files'


class TestLoadPackages(unittest.TestCase):

    def test_load(self):
        """Test loading packages from a configuration dictionary."""
        os.environ[env.VIZIERSERVER_PACKAGE_PATH] = PACKAGES_DIR
        config = AppConfig()
        packages = load_packages(config.engine.package_path)
        for key in [PACKAGE_PLOT, PACKAGE_PYTHON, PACKAGE_VIZUAL]:
            self.assertTrue(key in packages)


if __name__ == '__main__':
    unittest.main()
