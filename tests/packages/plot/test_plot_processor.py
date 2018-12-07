"""Test default implementation for vizual package task processor."""

import os
import shutil
import unittest

from vizier.client.command.plot import create_plot
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.packages.plot.processor import PlotProcessor
from vizier.engine.packages.processor import TaskContext
from vizier.filestore.fs.base import DefaultFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './.files/dataset.csv'
TSV_FILE = './.files/w49k-mmkh.tsv'

DATASET_NAME = 'abc'


class TestDefaultVizualProcessor(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default vizier processor for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.datastore=FileSystemDatastore(DATASTORE_DIR)
        self.filestore=DefaultFilestore(FILESTORE_DIR)

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
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        chart = result.outputs.stdout[0].value
        self.assertEquals(chart['data']['data'][0]['label'], 'A')
        self.assertEquals(chart['data']['data'][1]['label'], 'Series 2')
        self.assertEquals(chart['result']['series'][0]['label'], 'A')
        self.assertEquals(chart['result']['series'][1]['label'], 'Series 2')
        self.assertEquals(len(chart['result']['series'][0]['data']), 6)
        self.assertEquals(len(chart['result']['series'][1]['data']), 6)

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
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )

if __name__ == '__main__':
    unittest.main()
