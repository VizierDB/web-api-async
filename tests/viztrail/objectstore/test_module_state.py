import os
import shutil
import unittest

from vizier.core.timestamp import get_current_time
from vizier.datastore.dataset import DatasetDescriptor
from vizier.viztrail.objectstore.module import OSModuleHandle
from vizier.viztrail.module.base import MODULE_PENDING
from vizier.viztrail.module.output import ModuleOutputs, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.engine.packages.pycell.command import python_cell

MODULE_DIR = './.temp'

DS1 = DatasetDescriptor(identifier='ID1', name='ID1')
DS2 = DatasetDescriptor(identifier='ID2', name='ID2')


class TestModuleState(unittest.TestCase):

    def setUp(self):
        """Create an empty directory."""
        if os.path.isdir(MODULE_DIR):
            shutil.rmtree(MODULE_DIR)
        os.makedirs(MODULE_DIR)

    def tearDown(self):
        """Delete directory.
        """
        shutil.rmtree(MODULE_DIR)

    def test_canceled(self):
        """Update module state from pending to running."""
        # Create original module
        module = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            module_folder=MODULE_DIR,
            outputs=ModuleOutputs(stdout=[TextOutput('ABC')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')},
                resources={'fileid': '0123456789'}
            ),
            timestamp=ModuleTimestamp()
        )
        module.set_canceled()
        self.assertTrue(module.is_canceled)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')
        # Read module from object store and ensure that tall changes have been
        # materialized properly
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_canceled)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')
        # Set canceled with timestamp and output information
        ts = get_current_time()
        module.set_canceled(
            finished_at=ts,
            outputs=ModuleOutputs(stderr=[TextOutput('Some Error')])
        )
        self.assertTrue(module.is_canceled)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(module.timestamp.finished_at, ts)
        self.assertEqual(len(module.outputs.stderr), 1)
        self.assertEqual(module.outputs.stderr[0].value, 'Some Error')
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_canceled)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(module.timestamp.finished_at, ts)
        self.assertEqual(len(module.outputs.stderr), 1)
        self.assertEqual(module.outputs.stderr[0].value, 'Some Error')
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')

    def test_error(self):
        """Update module state from pending to error."""
        # Create original module
        module = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            module_folder=MODULE_DIR,
            outputs=ModuleOutputs(stdout=[TextOutput('ABC')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')},
                resources={'fileid': '0123456789'}
            ),
            timestamp=ModuleTimestamp()
        )
        module.set_error()
        self.assertTrue(module.is_error)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')
        # Read module from object store and ensure that tall changes have been
        # materialized properly
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_error)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')
        # Set canceled with timestamp and output information
        ts = get_current_time()
        module.set_error(
            finished_at=ts,
            outputs=ModuleOutputs(stderr=[TextOutput('Some Error')])
        )
        self.assertTrue(module.is_error)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(module.timestamp.finished_at, ts)
        self.assertEqual(len(module.outputs.stderr), 1)
        self.assertEqual(module.outputs.stderr[0].value, 'Some Error')
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_error)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(module.timestamp.finished_at, ts)
        self.assertEqual(len(module.outputs.stderr), 1)
        self.assertEqual(module.outputs.stderr[0].value, 'Some Error')
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        self.assertEqual(module.provenance.resources['fileid'], '0123456789')

    def test_running(self):
        """Update module state from pending to running."""
        # Create original module
        module = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            module_folder=MODULE_DIR,
            timestamp=ModuleTimestamp(),
            outputs=ModuleOutputs(stdout=[TextOutput('ABC')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')},
                resources={'fileid': '0123456789'}
            )
        )
        self.assertTrue(module.is_pending)
        module.set_running(external_form='TEST MODULE')
        self.assertTrue(module.is_running)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        # Read module from object store and ensure that tall changes have been
        # materialized properly
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_running)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertIsNotNone(module.provenance.read)
        self.assertIsNotNone(module.provenance.write)
        self.assertIsNotNone(module.provenance.resources)
        # Set running with all optional parameters
        module.set_running(
            started_at=module.timestamp.created_at,
            external_form='Some form'
        )
        self.assertEqual(module.timestamp.started_at, module.timestamp.created_at)
        self.assertEqual(module.external_form, 'Some form')
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertEqual(module.timestamp.started_at, module.timestamp.created_at)
        self.assertEqual(module.external_form, 'Some form')

    def test_safe_write(self):
        """Update module state with write error."""
        # Create original module
        module = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            module_folder=MODULE_DIR,
            timestamp=ModuleTimestamp(),
            outputs=ModuleOutputs(stdout=[TextOutput('ABC')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')}
            )
        )
        self.assertTrue(module.is_pending)
        module.set_running(external_form='TEST MODULE')
        self.assertTrue(module.is_running)
        module.set_success(outputs=ModuleOutputs(stderr=[None]))
        self.assertTrue(module.is_error)
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_running)

    def test_success(self) -> None:
        """Update module state from pending to success."""
        # Create original module
        module = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            module_folder=MODULE_DIR,
            timestamp=ModuleTimestamp(),
            outputs=ModuleOutputs(stdout=[TextOutput('ABC')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')}
            )
        )
        self.assertTrue(module.is_pending)
        module.set_running(external_form='TEST MODULE')
        module.set_success()
        self.assertTrue(module.is_success)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertTrue(module.provenance.read is None)
        self.assertTrue(module.provenance.write is None)
        # Read module from object store and ensure that tall changes have been
        # materialized properly
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        self.assertTrue(module.is_success)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 0)
        self.assertTrue(module.provenance.read is None)
        self.assertTrue(module.provenance.write is None)
        # Set success with all optional parameters
        ts = get_current_time()
        module.set_success(
            finished_at=ts,
            outputs=ModuleOutputs(stdout=[TextOutput('XYZ')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')}
            )
        )
        self.assertTrue(module.is_success)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(module.timestamp.finished_at, ts)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 1)
        self.assertEqual(module.outputs.stdout[0].value, 'XYZ')
        self.assertIsNotNone(module.provenance.read)
        self.assertEqual(module.provenance.read['DS1'], 'ID1')
        self.assertIsNotNone(module.provenance.write)
        self.assertEqual(module.provenance.write['DS1'].identifier, 'ID2')
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path
        )
        module = OSModuleHandle.load_module(
            identifier=module.identifier,
            module_path=module.module_path,
            prev_state=dict()
        )
        self.assertTrue(module.is_success)
        self.assertIsNotNone(module.timestamp.started_at)
        self.assertIsNotNone(module.timestamp.finished_at)
        self.assertEqual(module.timestamp.finished_at, ts)
        self.assertEqual(len(module.outputs.stderr), 0)
        self.assertEqual(len(module.outputs.stdout), 1)
        self.assertEqual(module.outputs.stdout[0].value, 'XYZ')
        self.assertIsNotNone(module.provenance.read)
        self.assertEqual(module.provenance.read['DS1'], 'ID1')
        self.assertIsNotNone(module.provenance.write)
        self.assertEqual(module.provenance.write['DS1'].identifier, 'ID2')

    def test_state(self):
        """Ensure that only one of the state flag is True at the same time."""
        # Create original module
        module = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            module_folder=MODULE_DIR,
            timestamp=ModuleTimestamp(),
            outputs=ModuleOutputs(stdout=[TextOutput('ABC')]),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': DatasetDescriptor(identifier='ID2', name='ID2')}
            )
        )
        # Pending
        self.assertTrue(module.is_pending)
        self.assertFalse(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertFalse(module.is_running)
        self.assertFalse(module.is_success)
        # Running
        module.set_running(external_form='TEST MODULE')
        self.assertFalse(module.is_pending)
        self.assertFalse(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertTrue(module.is_running)
        self.assertFalse(module.is_success)
        # Canceled
        module.set_canceled()
        self.assertFalse(module.is_pending)
        self.assertTrue(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertFalse(module.is_running)
        self.assertFalse(module.is_success)
        # Error
        module.set_error()
        self.assertFalse(module.is_pending)
        self.assertFalse(module.is_canceled)
        self.assertTrue(module.is_error)
        self.assertFalse(module.is_running)
        self.assertFalse(module.is_success)
        # Success
        module.set_success()
        self.assertFalse(module.is_pending)
        self.assertFalse(module.is_canceled)
        self.assertFalse(module.is_error)
        self.assertFalse(module.is_running)
        self.assertTrue(module.is_success)


if __name__ == '__main__':
    unittest.main()
