"""Test validation of pre-defined package declarations."""

import unittest

from vizier.engine.packages.mimir.base import MIMIR_LENSES
from vizier.engine.packages.plot.base import PLOT_COMMANDS
from vizier.engine.packages.base import validate_package
from vizier.engine.packages.pycell.base import PYTHON_COMMANDS
from vizier.engine.packages.vizual.base import VIZUAL_COMMANDS


class TestValidatePackage(unittest.TestCase):

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
