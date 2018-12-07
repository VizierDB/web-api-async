"""Test default implementation for vizual package task processor."""

import os
import shutil
import unittest

from vizier.client.command.plot import create_plot
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.packages.plot.query import ChartQuery
from vizier.engine.packages.plot.view import ChartViewHandle
from vizier.filestore.fs.base import DefaultFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'

LOAD_FILE = './.files/w49k-mmkh.tsv'

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

    def count_non_null_values(self, data, column_index):
        """Return the number of values in a column that are not None."""
        count = 0
        for row in data:
            if not row[column_index] is None:
                count += 1
        return count

    def test_query(self):
        """Test running a query for simple chart plots."""
        ds = self.datastore.load_dataset(self.filestore.upload_file(LOAD_FILE))
        view = ChartViewHandle(dataset_name='ABC', x_axis=2)
        view.add_series(1, range_start=25, range_end=30)
        view.add_series(0, range_start=25, range_end=30)
        view.add_series(3, range_start=25, range_end=30)
        data = ChartQuery().exec_query(dataset=ds, view=view)
        self.assertEquals(len(data), 6)
        for row in data:
            self.assertEquals(len(row), 3)
        self.assertTrue(isinstance(data[0][0], int))
        self.assertTrue(isinstance(data[0][1], float))
        # Remove interval end for one series. This should return all rows
        # starting from index 25
        view = ChartViewHandle(dataset_name='ABC', x_axis=2)
        view.add_series(1, range_start=25, range_end=30)
        view.add_series(0, range_start=25)
        view.add_series(3, range_start=25, range_end=30)
        data = ChartQuery().exec_query(dataset=ds, view=view)
        self.assertEquals(len(data), 29)
        self.assertIsNone(data[28][0])
        self.assertIsNotNone(data[28][1])
        self.assertIsNone(data[28][2])
        for row in data:
            self.assertEquals(len(row), 3)
        # Remove interval start for another series. The first series will
        # contain 31 values, the second 29, and the third 6
        view = ChartViewHandle(dataset_name='ABC', x_axis=2)
        view.add_series(1, range_end=30)
        view.add_series(0, range_start=25)
        view.add_series(3, range_start=25, range_end=30)
        data = ChartQuery().exec_query(dataset=ds, view=view)
        self.assertEquals(len(data), 31)
        self.assertEquals(self.count_non_null_values(data, 0), 31)
        self.assertEquals(self.count_non_null_values(data, 1), 29)
        self.assertEquals(self.count_non_null_values(data, 2), 6)
        for row in data:
            self.assertEquals(len(row), 3)
        # Without any range constraints the result should contain all 54 rows
        view = ChartViewHandle(dataset_name='ABC', x_axis=2)
        view.add_series(1, label='A')
        view.add_series(0, label='B')
        view.add_series(3)
        data = ChartQuery().exec_query(dataset=ds, view=view)
        self.assertEquals(len(data), 54)


if __name__ == '__main__':
    unittest.main()
