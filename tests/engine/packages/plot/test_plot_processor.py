"""Test default implementation for vizual package task processor."""

import os
import shutil
import unittest

from vizier.engine.packages.plot.command import create_plot
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.packages.plot.processor import PlotProcessor
from vizier.engine.task.base import TaskContext
from vizier.filestore.fs.base import FileSystemFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './tests/engine/packages/plot/.files/dataset.csv'
TSV_FILE = './tests/engine/packages/plot/.files/w49k-mmkh.tsv'

DATASET_NAME = 'abc'


class TestDefaultPlotProcessor(unittest.TestCase):

    def setUp(self):
        """Create instances of the default datastore and filestore."""
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.datastore=FileSystemDatastore(DATASTORE_DIR)
        self.filestore=FileSystemFilestore(FILESTORE_DIR)

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_advanced_plot(self):
        """Test running the simple plot command with a more advanced chart
        definition.
        """
        fh = self.filestore.upload_file(TSV_FILE)
        ds = self.datastore.load_dataset(fh)
        cmd = create_plot(
            dataset_name=DATASET_NAME,
            chart_name='My Chart',
            series=[{'column': 1, 'range': '25:30', 'label': 'A'}, {'column': 0, 'range': '25:30'}],
            validate=True
        )
        result = PlotProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                project_id=0,
                datastore=self.datastore,
                filestore=self.filestore,
                artifacts={DATASET_NAME: ds}
            )
        )
        chart = result.outputs.stdout[0].value
        self.assertEqual(chart['data']['data'][0]['label'], 'A')
        self.assertEqual(chart['data']['data'][1]['label'], 'average_class_size')
        self.assertEqual(chart['result']['series'][0]['label'], 'A')
        self.assertEqual(chart['result']['series'][1]['label'], 'average_class_size')
        self.assertEqual(len(chart['result']['series'][0]['data']), 6)
        self.assertEqual(len(chart['result']['series'][1]['data']), 6)

    def test_simple_plot(self):
        """Test running the simple plot command."""
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.datastore.load_dataset(fh)
        cmd = create_plot(
            dataset_name=DATASET_NAME,
            chart_name='My Chart',
            series=[{'column': 1}],
            validate=True
        )
        result = PlotProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                project_id=0,
                datastore=self.datastore,
                filestore=self.filestore,
                artifacts={DATASET_NAME: ds}
            )
        )

if __name__ == '__main__':
    unittest.main()
