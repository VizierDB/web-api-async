"""Test functionality of the dataset descriptor."""

import os
import shutil
import unittest

from werkzeug.datastructures import FileStorage

from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor


class TestDatasetDescriptor(unittest.TestCase):

    def test_column_index(self):
        """Test access to columns based on identifier and name."""
        ds = DatasetDescriptor(
            identifier='0',
            columns=[
                DatasetColumn(identifier=0, name='ABC'),
                DatasetColumn(identifier=1, name='A'),
                DatasetColumn(identifier=2, name='ABC'),
                DatasetColumn(identifier=3, name='DEF'),
                DatasetColumn(identifier=4, name='xyz'),
            ]
        )
        # Get column by identifier
        self.assertEquals(ds.column_by_id(0).name, 'ABC')
        self.assertEquals(ds.column_by_id(1).name, 'A')
        self.assertEquals(ds.column_by_id(2).name, 'ABC')
        self.assertEquals(ds.column_by_id(3).name, 'DEF')
        self.assertEquals(ds.column_by_id(4).name, 'xyz')
        with self.assertRaises(ValueError):
            ds.column_by_id(6)
        with self.assertRaises(ValueError):
            ds.column_by_id(-1)
        # Get column by name
        self.assertEquals(ds.column_by_name('ABC').identifier, 0)
        self.assertEquals(ds.column_by_name('A').identifier, 1)
        self.assertEquals(ds.column_by_name('abc', ignore_case=True).identifier, 0)
        self.assertEquals(ds.column_by_name('XYZ', ignore_case=True).identifier, 4)
        self.assertIsNone(ds.column_by_name('4'))
        # Get column index
        self.assertEquals(ds.column_index(0), 0)
        self.assertEquals(ds.column_index(1), 1)
        self.assertEquals(ds.column_index('DEF'), 3)
        self.assertEquals(ds.column_index('XYZ'), 4)
        self.assertEquals(ds.column_index('A'), 1)
        self.assertEquals(ds.column_index('B'), 1)
        self.assertEquals(ds.column_index('C'), 2)
        self.assertEquals(ds.column_index('D'), 3)
        self.assertEquals(ds.column_index('E'), 4)
        with self.assertRaises(ValueError):
            ds.column_index('ABC')
        with self.assertRaises(ValueError):
            ds.column_index('abc')


if __name__ == '__main__':
    unittest.main()
