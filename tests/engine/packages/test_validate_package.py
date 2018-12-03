"""Test validation of pre-defined package declarations."""

import unittest

from vizier.config import AppConfig
from vizier.engine.packages.mimir.base import MIMIR_LENSES
from vizier.engine.packages.plot.base import PLOT_COMMANDS
from vizier.engine.packages.base import validate_package
from vizier.engine.packages.pycell.base import PYTHON_COMMANDS
from vizier.engine.packages.vizual.base import VIZUAL_COMMANDS


class TestValidatePackage(unittest.TestCase):

    def test_validate_app_packages(self):
        """Validate packages that are contained in the application
        configuration.
        """
        # Validate the default packages upon loading the configuration file
        AppConfig(configuration_file='./.files/config.yaml')
        # Load configuration file that references the default MIMIR, PLOT and
        # PYTHON package
        AppConfig(configuration_file='./.files/config-all-packages.yaml')

    def test_validate_erroneous_package(self):
        """Ensure that an exception is raised if a referenced package is
        invalid.
        """
        with self.assertRaises(ValueError):
            AppConfig(configuration_file='./.files/config-with-erroneous-package.yaml')

    def test_validate_mimir(self):
        """Validate declaration of the mimir lenses."""
        validate_package(MIMIR_LENSES)

    def test_validate_plot(self):
        """Validate declaration of the plot package."""
        validate_package(PLOT_COMMANDS)

    def test_validate_python(self):
        """Validate declaration of the python package."""
        validate_package(PYTHON_COMMANDS)

    def test_validate_vizual(self):
        """Validate declaration of the vizual package."""
        validate_package(VIZUAL_COMMANDS)


if __name__ == '__main__':
    unittest.main()
