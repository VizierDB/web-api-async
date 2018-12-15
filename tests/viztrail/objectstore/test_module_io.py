"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.core.timestamp import get_current_time, to_datetime
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.viztrail.driver.objectstore.module import OSModuleHandle
from vizier.viztrail.module.base import MODULE_PENDING
from vizier.viztrail.module.output import ModuleOutputs, OutputObject, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.api.client.command.plot import create_plot
from vizier.api.client.command.pycell import python_cell


MODULE_DIR = './.temp'


DATASETS = {
    'DS1': DatasetDescriptor(identifier='ID1'),
    'DS2': DatasetDescriptor(
        identifier='ID2',
        columns=[
            DatasetColumn(identifier=0, name='ABC'),
            DatasetColumn(identifier=1, name='xyz')
        ],
        row_count=100
    )
}


class TestOSModuleIO(unittest.TestCase):

    def setUp(self):
        """Create an empty directory."""
        if os.path.isdir(MODULE_DIR):
            shutil.rmtree(MODULE_DIR)
        os.makedirs(MODULE_DIR)

    def tearDown(self):
        """Delete directory.
        """
        shutil.rmtree(MODULE_DIR)

    def test_datasets(self):
        """Test reading and writing modules with dataset information."""
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(len(m.datasets), 2)
        self.assertEquals(m.datasets['DS1'].identifier, 'ID1')
        self.assertEquals(len(m.datasets['DS1'].columns), 0)
        self.assertEquals(m.datasets['DS1'].row_count, 0)
        self.assertEquals(m.datasets['DS2'].identifier, 'ID2')
        self.assertEquals(len(m.datasets['DS2'].columns), 2)
        self.assertEquals(m.datasets['DS2'].columns[0].identifier, 0)
        self.assertEquals(m.datasets['DS2'].columns[0].name, 'ABC')
        self.assertEquals(m.datasets['DS2'].columns[1].identifier, 1)
        self.assertEquals(m.datasets['DS2'].columns[1].name, 'xyz')
        self.assertEquals(m.datasets['DS2'].row_count, 100)

    def test_outputs(self):
        """Test reading and writing modules with output information."""
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(len(m.outputs.stderr), 0)
        self.assertEquals(len(m.outputs.stdout), 0)
        # Module with error output
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(stderr=[TextOutput('Some text')]),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(len(m.outputs.stderr), 1)
        self.assertTrue(m.outputs.stderr[0].is_text)
        self.assertEquals(m.outputs.stderr[0].value, 'Some text')
        self.assertEquals(len(m.outputs.stdout), 0)
        # Module with standard output
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(stdout=[TextOutput('Some text'), OutputObject(type='chart', value='123')]),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(len(m.outputs.stdout), 2)
        self.assertTrue(m.outputs.stdout[0].is_text)
        self.assertEquals(m.outputs.stdout[0].value, 'Some text')
        self.assertFalse(m.outputs.stdout[1].is_text)
        self.assertEquals(m.outputs.stdout[1].value, '123')
        self.assertEquals(len(m.outputs.stderr), 0)
        # Module with standard error and standard output
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(
                stderr=[TextOutput('Some text')],
                stdout=[TextOutput('Some text'), OutputObject(type='chart', value='123')]
            ),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(len(m.outputs.stdout), 2)
        self.assertEquals(len(m.outputs.stderr), 1)

    def test_provenance(self):
        """Test reading and writing modules with provenance information."""
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNone(m.provenance.read)
        self.assertIsNone(m.provenance.write)
        # Modules that only has read provenance
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                resources={'fileId': '0123456789'}
            ),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNotNone(m.provenance.read)
        self.assertEquals(len(m.provenance.read), 1)
        self.assertEquals(m.provenance.read['DS1'], 'ID1')
        self.assertEquals(m.provenance.resources['fileId'], '0123456789')
        self.assertIsNone(m.provenance.write)
        # Modules that only has write provenance
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(
                write={'DS2': 'ID2'}
            ),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNotNone(m.provenance.write)
        self.assertEquals(len(m.provenance.write), 1)
        self.assertEquals(m.provenance.write['DS2'], 'ID2')
        self.assertIsNone(m.provenance.read)
        # Module with read and write provenance
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write={'DS1': 'ID1A','DS2': 'ID2'},
                delete=['A', 'B']
            ),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNotNone(m.provenance.read)
        self.assertEquals(len(m.provenance.read), 1)
        self.assertEquals(m.provenance.read['DS1'], 'ID1')
        self.assertIsNotNone(m.provenance.write)
        self.assertEquals(len(m.provenance.write), 2)
        self.assertEquals(m.provenance.write['DS1'], 'ID1A')
        self.assertEquals(m.provenance.write['DS2'], 'ID2')
        self.assertEquals(m.provenance.delete, ['A', 'B'])

    def test_read_write_module(self):
        """Test reading and writing modules."""
        mod0 = OSModuleHandle.create_module(
            command=create_plot(
                dataset_name='dataset',
                chart_name='My Chart',
                series=[
                    {'column': 1, 'range': '0:50', 'label': 'A'},
                    {'column': 2, 'range': '51:100', 'label': 'B'},
                    {'column': 3,'label': 'C'},
                    {'column': 4}
                ],
                chart_type='bar',
                chart_grouped=False,
                xaxis_range='0:100',
                xaxis_column=None,
            ),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertTrue(os.path.isfile(m.module_path))
        self.assertEquals(m.external_form, 'TEST MODULE')
        self.assertTrue(m.is_pending)

    def test_timestamps(self):
        """Test reading and writing modules with different timestamp values."""
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        # Test timestamps
        created_at = m.timestamp.created_at
        started_at = to_datetime('2018-11-26T13:00:00.000000')
        m.timestamp.started_at = started_at
        m.write_module()
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(m.timestamp.created_at, created_at)
        self.assertEquals(m.timestamp.started_at, started_at)
        finished_at = to_datetime('2018-11-26T13:00:00.000010')
        m.timestamp.created_at = finished_at
        m.timestamp.finished_at = finished_at
        m.write_module()
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(m.timestamp.created_at, finished_at)
        self.assertEquals(m.timestamp.started_at, started_at)
        self.assertEquals(m.timestamp.finished_at, finished_at)
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(
                created_at=created_at,
                started_at=started_at
            ),
            module_folder=MODULE_DIR,
            datasets=DATASETS
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEquals(m.timestamp.created_at, created_at)
        self.assertEquals(m.timestamp.started_at, started_at)
        self.assertIsNone(m.timestamp.finished_at)


if __name__ == '__main__':
    unittest.main()
