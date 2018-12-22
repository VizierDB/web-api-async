"""Test the execute method of the synchronous backend."""

import os
import shutil
import time
import unittest

from vizier.api.client.command.pycell import python_cell
from vizier.api.client.command.vizual import update_cell
from vizier.core.loader import ClassLoader
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.packages.plot.processor import PlotProcessor
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.processor import VizualTaskProcessor

import vizier.engine.backend.multiprocess as mp
import vizier.engine.packages.plot.base as plot
import vizier.engine.packages.pycell.base as pycell
import vizier.engine.packages.vizual.base as vizual
import vizier.engine.packages.vizual.processor as vzproc


class TestMultiprocessBackendInit(unittest.TestCase):

    def test_init(self):
        """Test creating an instance of the backend from a given dictionary of
        class loaders.
        """
        processors = {
            pycell.PACKAGE_PYTHON: PyCellTaskProcessor(),
            vizual.PACKAGE_VIZUAL:  PyCellTaskProcessor()
        }
        synchron_commands = {
            vizual.PACKAGE_VIZUAL: {
                vizual.VIZUAL_UPD_CELL: PyCellTaskProcessor(),
                vizual.VIZUAL_LOAD: PyCellTaskProcessor()
            },
        }
        backend = MultiProcessBackend(
            processors=processors,
            synchron_commands=synchron_commands
        )
        self.assertTrue(len(backend.processors), 2)
        self.assertTrue(len(backend.commands), 1)
        self.assertTrue(len(backend.commands[vizual.PACKAGE_VIZUAL]), 2)
        self.assertTrue(isinstance(backend.processors[pycell.PACKAGE_PYTHON], PyCellTaskProcessor))
        self.assertTrue(isinstance(backend.processors[vizual.PACKAGE_VIZUAL], PyCellTaskProcessor))
        self.assertTrue(isinstance(backend.commands[vizual.PACKAGE_VIZUAL][vizual.VIZUAL_UPD_CELL], PyCellTaskProcessor))
        self.assertTrue(isinstance(backend.commands[vizual.PACKAGE_VIZUAL][vizual.VIZUAL_LOAD], PyCellTaskProcessor))
        # Initialize the backend with additional properties that override some
        # of the pre-defined processors and synchronous commands.
        properties = {
            mp.PROPERTY_PROCESSORS: {
                vizual.PACKAGE_VIZUAL: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.vizual.processor',
                    class_name='VizualTaskProcessor',
                    properties={
                        vzproc.PROPERTY_API: ClassLoader.to_dict(
                            module_name='vizier.engine.packages.vizual.api.fs',
                            class_name='DefaultVizualApi'
                        )
                    }
                ),
                plot.PACKAGE_PLOT: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.plot.processor',
                    class_name='PlotProcessor'
                )
            },
            mp.PROPERTY_SYNCHRON: {
                vizual.PACKAGE_VIZUAL: {
                    vizual.VIZUAL_UPD_CELL: ClassLoader.to_dict(
                        module_name='vizier.engine.packages.vizual.processor',
                        class_name='VizualTaskProcessor',
                        properties={
                            vzproc.PROPERTY_API: ClassLoader.to_dict(
                                module_name='vizier.engine.packages.vizual.api.fs',
                                class_name='DefaultVizualApi'
                            )
                        }
                    )
                }
            }
        }
        backend = MultiProcessBackend(
            processors=processors,
            synchron_commands=synchron_commands,
            properties=properties
        )
        self.assertTrue(len(backend.processors), 3)
        self.assertTrue(len(backend.commands), 1)
        self.assertTrue(len(backend.commands[vizual.PACKAGE_VIZUAL]), 2)
        self.assertTrue(isinstance(backend.processors[pycell.PACKAGE_PYTHON], PyCellTaskProcessor))
        self.assertTrue(isinstance(backend.processors[plot.PACKAGE_PLOT], PlotProcessor))
        self.assertTrue(isinstance(backend.processors[vizual.PACKAGE_VIZUAL], VizualTaskProcessor))
        self.assertTrue(isinstance(backend.commands[vizual.PACKAGE_VIZUAL][vizual.VIZUAL_UPD_CELL], VizualTaskProcessor))
        self.assertTrue(isinstance(backend.commands[vizual.PACKAGE_VIZUAL][vizual.VIZUAL_LOAD], PyCellTaskProcessor))
        proc = backend.commands[vizual.PACKAGE_VIZUAL][vizual.VIZUAL_UPD_CELL]
        self.assertTrue(isinstance(proc.api, DefaultVizualApi))
        self.assertTrue(
            backend.can_execute(
                update_cell(
                    dataset_name='ds',
                    column=1,
                    row=0,
                    value=9,
                    validate=True
                )
            )
        )
        self.assertFalse(
            backend.can_execute(
                python_cell(
                    source='print 2+2',
                    validate=True
                )
            )
        )

        
if __name__ == '__main__':
    unittest.main()
