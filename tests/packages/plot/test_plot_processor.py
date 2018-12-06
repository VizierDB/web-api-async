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

    def test_simple_plot(self):
        """Test running the simpl eplot command."""
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
