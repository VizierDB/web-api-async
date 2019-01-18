"""Test functionality of the dataset descriptor."""

import os
import shutil
import unittest

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
        for i in range(len(ds.columns)):
            self.assertEquals(ds.get_index(i), i)
        with self.assertRaises(ValueError):
            ds.column_index('ABC')
        with self.assertRaises(ValueError):
            ds.column_index('abc')
        # Create a descriptor when column identifier does not match the index
        # position in the schema
        ds = DatasetDescriptor(
            identifier='0',
            columns=[
                DatasetColumn(identifier=4, name='ABC'),
                DatasetColumn(identifier=2, name='A'),
                DatasetColumn(identifier=3, name='ABC'),
                DatasetColumn(identifier=0, name='DEF'),
                DatasetColumn(identifier=1, name='xyz'),
            ]
        )
        self.assertEquals(ds.column_by_id(0).name, 'DEF')
        self.assertEquals(ds.column_by_id(1).name, 'xyz')
        self.assertEquals(ds.column_by_id(2).name, 'A')
        self.assertEquals(ds.column_by_id(3).name, 'ABC')
        self.assertEquals(ds.column_by_id(4).name, 'ABC')
        self.assertEquals(ds.column_index(0), 0)
        self.assertEquals(ds.column_index(1), 1)
        self.assertEquals(ds.column_index('DEF'), 3)
        self.assertEquals(ds.column_index('XYZ'), 4)
        self.assertEquals(ds.column_index('A'), 1)
        self.assertEquals(ds.column_index('B'), 1)
        self.assertEquals(ds.column_index('C'), 2)
        self.assertEquals(ds.column_index('D'), 3)
        self.assertEquals(ds.column_index('E'), 4)
        self.assertEquals(ds.get_index(0), 3)
        self.assertEquals(ds.get_index(1), 4)
        self.assertEquals(ds.get_index(2), 1)
        self.assertEquals(ds.get_index(3), 2)
        self.assertEquals(ds.get_index(4), 0)

    def test_unique_name(self):
        """Test method that computes unique column names."""
        ds = DatasetDescriptor(
            identifier='0',
            columns=[
                DatasetColumn(identifier=0, name='ABC'),
                DatasetColumn(identifier=1, name='A'),
                DatasetColumn(identifier=2, name='ABC_1'),
                DatasetColumn(identifier=3, name='DEF'),
                DatasetColumn(identifier=4, name='xyz'),
            ]
        )
        self.assertEquals(ds.get_unique_name('Age'), 'Age')
        self.assertEquals(ds.get_unique_name('XYZ'), 'XYZ_1')
        self.assertEquals(ds.get_unique_name('xyz'), 'xyz_1')
        self.assertEquals(ds.get_unique_name('ABC'), 'ABC_2')


if __name__ == '__main__':
    unittest.main()
