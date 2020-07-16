"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.core.timestamp import get_current_time, to_datetime
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.view.chart import ChartViewHandle, DataSeriesHandle
from vizier.viztrail.objectstore.module import OSModuleHandle
from vizier.viztrail.module.base import MODULE_PENDING, MODULE_SUCCESS
from vizier.viztrail.module.output import ModuleOutputs, OutputObject, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.engine.packages.plot.command import create_plot
from vizier.engine.packages.pycell.command import python_cell
from vizier.engine.base import compute_context

MODULE_DIR = './.temp'


DATASETS = {
    'DS1': DatasetDescriptor(identifier='ID1'),
    'DS2': DatasetDescriptor(
        identifier='ID2',
        columns=[
            DatasetColumn(identifier=0, name='ABC', data_type='int'),
            DatasetColumn(identifier=1, name='xyz', data_type='real')
        ]
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

    def test_outputs(self):
        """Test reading and writing modules with output information."""
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(len(m.outputs.stderr), 0)
        self.assertEqual(len(m.outputs.stdout), 0)
        # Module with error output
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(stderr=[TextOutput('Some text')]),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(len(m.outputs.stderr), 1)
        self.assertTrue(m.outputs.stderr[0].is_text)
        self.assertEqual(m.outputs.stderr[0].value, 'Some text')
        self.assertEqual(len(m.outputs.stdout), 0)
        # Module with standard output
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(stdout=[TextOutput('Some text'), OutputObject(type='chart', value='123')]),
            provenance=ModuleProvenance(),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(len(m.outputs.stdout), 2)
        self.assertTrue(m.outputs.stdout[0].is_text)
        self.assertEqual(m.outputs.stdout[0].value, 'Some text')
        self.assertFalse(m.outputs.stdout[1].is_text)
        self.assertEqual(m.outputs.stdout[1].value, '123')
        self.assertEqual(len(m.outputs.stderr), 0)
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
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(len(m.outputs.stdout), 2)
        self.assertEqual(len(m.outputs.stderr), 1)

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
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNone(m.provenance.read)
        self.assertIsNone(m.provenance.write)
        self.assertIsNone(m.provenance.delete)
        self.assertIsNone(m.provenance.resources)
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
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNotNone(m.provenance.read)
        self.assertEqual(len(m.provenance.read), 1)
        self.assertEqual(m.provenance.read['DS1'], 'ID1')
        self.assertEqual(m.provenance.resources['fileId'], '0123456789')
        self.assertIsNone(m.provenance.write)
        # Modules that only has write provenance
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(
                write=DATASETS
            ),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNotNone(m.provenance.write)
        self.assertEqual(len(m.provenance.write), 2)
        self.assertEqual(m.provenance.write['DS1'].identifier, 'ID1')
        self.assertEqual(m.provenance.write['DS2'].identifier, 'ID2')
        self.assertIsNone(m.provenance.read)
        # Module with read and write provenance
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(
                read={'DS1': 'ID1'},
                write=DATASETS,
                delete=['A', 'B']
            ),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertIsNotNone(m.provenance.read)
        self.assertEqual(len(m.provenance.read), 1)
        self.assertEqual(m.provenance.read['DS1'], 'ID1')
        self.assertIsNotNone(m.provenance.write)
        self.assertEqual(len(m.provenance.write), 2)
        self.assertEqual(m.provenance.write['DS1'].identifier, 'ID1')
        self.assertEqual(m.provenance.write['DS2'].identifier, 'ID2')
        self.assertEqual(m.provenance.delete, ['A', 'B'])
        # Module with chart
        chart = ChartViewHandle(
            identifier='A',
            dataset_name='DS1',
            chart_name='My Chart',
            data=[
                DataSeriesHandle(
                    column='COL1',
                    label='SERIES1',
                    range_start=0,
                    range_end=100
                ),
                DataSeriesHandle(
                    column='COL2',
                    range_start=101,
                    range_end=200
                ),
                DataSeriesHandle(
                    column='COL3',
                    label='SERIES2'
                )
            ],
            x_axis=1,
            chart_type='bar',
            grouped_chart=True
        )
        mod0 = OSModuleHandle.create_module(
            command=python_cell(source='print 2+2'),
            external_form='TEST MODULE',
            state=MODULE_PENDING,
            outputs=ModuleOutputs(),
            provenance=ModuleProvenance(
                charts=[chart]
            ),
            timestamp=ModuleTimestamp(),
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(len(m.provenance.charts), 1)
        c = m.provenance.charts[0]
        self.assertEqual(chart.identifier, c.identifier)
        self.assertEqual(chart.dataset_name, c.dataset_name)
        self.assertEqual(chart.chart_name, c.chart_name)
        self.assertEqual(chart.x_axis, c.x_axis)
        self.assertEqual(chart.chart_type, c.chart_type)
        self.assertEqual(chart.grouped_chart, c.grouped_chart)
        self.assertEqual(len(c.data), 3)
        for i in range(3):
            self.assertEqual(c.data[i].column, chart.data[i].column)
            self.assertEqual(c.data[i].label, chart.data[i].label)
            self.assertEqual(c.data[i].range_start, chart.data[i].range_start)
            self.assertEqual(c.data[i].range_end, chart.data[i].range_end)

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
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertTrue(os.path.isfile(m.module_path))
        self.assertEqual(m.external_form, 'TEST MODULE')
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
            module_folder=MODULE_DIR
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
        self.assertEqual(m.timestamp.created_at, created_at)
        self.assertEqual(m.timestamp.started_at, started_at)
        finished_at = to_datetime('2018-11-26T13:00:00.000010')
        m.timestamp.created_at = finished_at
        m.timestamp.finished_at = finished_at
        m.write_module()
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(m.timestamp.created_at, finished_at)
        self.assertEqual(m.timestamp.started_at, started_at)
        self.assertEqual(m.timestamp.finished_at, finished_at)
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
            module_folder=MODULE_DIR
        )
        m = OSModuleHandle.load_module(
            identifier=mod0.identifier,
            module_path=mod0.module_path
        )
        self.assertEqual(m.timestamp.created_at, created_at)
        self.assertEqual(m.timestamp.started_at, started_at)
        self.assertIsNone(m.timestamp.finished_at)


if __name__ == '__main__':
    unittest.main()
