import os
import unittest

from vizier.config.engine.base import load_packages
from vizier.config.engine.base import PARA_PACKAGE_DECLARATION, PARA_PACKAGE_ENGINE
from vizier.core.loader import ClassLoader
from vizier.engine.packages.plot.base import PACKAGE_PLOT
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL
from vizier.engine.packages.vizual.processor import PROPERTY_API


class TestLoadPackages(unittest.TestCase):

    def test_error(self):
        """Test loading packages from invalid configurations."""
        elements = [
            {
                PARA_PACKAGE_DECLARATION: './.files/pycell.pckg.json',
                PARA_PACKAGE_ENGINE: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.pycell.processor',
                    class_name='PyCellTaskProcessor'
                )
            },
            {
                PARA_PACKAGE_DECLARATION: './.files/plot.pckg.json',
                'somethingDifferent': ClassLoader.to_dict(
                    module_name='vizier.engine.packages.plot.processor',
                    class_name='PlotProcessor'
                )
            }
        ]
        with self.assertRaises(ValueError):
            packages, processors = load_packages(elements)

    def test_load(self):
        """Test loading packages from a configuration dictionary."""
        elements = [
            {
                PARA_PACKAGE_DECLARATION: './.files/pycell.pckg.json',
                PARA_PACKAGE_ENGINE: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.pycell.processor',
                    class_name='PyCellTaskProcessor'
                )
            },
            {
                PARA_PACKAGE_DECLARATION: './.files/plot.pckg.json',
                PARA_PACKAGE_ENGINE: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.plot.processor',
                    class_name='PlotProcessor'
                )
            },
            {
                PARA_PACKAGE_DECLARATION: './.files/vizual.pckg.json',
                PARA_PACKAGE_ENGINE: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.vizual.processor',
                    class_name='VizualTaskProcessor',
                    properties={
                        PROPERTY_API: ClassLoader.to_dict(
                            module_name='vizier.engine.packages.vizual.api.fs',
                            class_name='DefaultVizualApi'
                        )
                    }
                )
            }
        ]
        packages, processors = load_packages(elements)
        for key in [PACKAGE_PLOT, PACKAGE_PYTHON, PACKAGE_VIZUAL]:
            self.assertTrue(key in packages)
            self.assertTrue(key in processors)


if __name__ == '__main__':
    unittest.main()
