"""Test task processor for Mimir lenses."""

import os
import shutil
import unittest

from vizier.core.loader import ClassLoader
from vizier.engine.task.base import TaskContext

from vizier.datastore.mimir.store import MimirDatastore
from vizier.engine.packages.mimir.processor import MimirProcessor
from vizier.filestore.fs.base import FileSystemFilestore

import vizier.engine.packages.mimir.base as lens_types
import vizier.engine.packages.mimir.command as cmd
import vizier.engine.packages.base as pckg
import vizier.mimir as mimir


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'

CSV_FILE = './tests/engine/packages/mimir/.files/dataset.csv'
GEO_FILE = './tests/engine/packages/mimir/.files/geo.csv'
KEY_REPAIR_FILE = './tests/engine/packages/mimir/.files/key_repair.csv'
INCOMPLETE_CSV_FILE = './tests/engine/packages/mimir/.files/dataset_with_missing_values.csv'
PICKER_FILE = './tests/engine/packages/mimir/.files/dataset_pick.csv'

DATASET_NAME = 'dataset'


class TestMimirProcessor(unittest.TestCase):
    """Individual test for Mimir lenses. Run separately since each test has to
    initialize and shout down the Mimir gateway.
    """
    def setUp(self):
        """Create an instance of the Mimir processor for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.processor = MimirProcessor()
        self.datastore = MimirDatastore(DATASTORE_DIR)
        self.filestore = FileSystemFilestore(FILESTORE_DIR)
        self.available_lenses = set(mimir.getAvailableLensTypes())

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def compute_lens_result(self, ds, command):
        return self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                project_id=1,
                datastore=self.datastore,
                filestore=self.filestore,
                artifacts={DATASET_NAME: ds}
            )
        )

    def test_geocode_lens(self):
        if lens_types.MIMIR_GEOCODE not in self.available_lenses:
            self.skipTest("Mimir Geocoding Lens not initialized.")
        """Test GEOCODE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        f_handle = self.filestore.upload_file(GEO_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Geocode Lens
        command=cmd.mimir_geocode(
            DATASET_NAME,
            'GOOGLE',
            house_nr=ds.column_by_name('STRNUMBER').identifier,
            street=ds.column_by_name('STRNAME').identifier,
            city=ds.column_by_name('CITY').identifier,
            state=ds.column_by_name('STATE').identifier
        )
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)


        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        self.assertEqual(len(columns), 6)
        self.assertTrue('LATITUDE' in columns)
        self.assertTrue('LONGITUDE' in columns)

        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        self.assertEqual(len(columns), 8)
        self.assertTrue('LATITUDE_1' in columns)
        self.assertTrue('LONGITUDE_1' in columns)
        self.assertEqual(len(ds.columns), 8)

    def test_key_repair_lens(self):
        """Test KEY REPAIR lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        f_handle = self.filestore.upload_file(KEY_REPAIR_FILE)
        ds1 = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        command = cmd.mimir_key_repair(DATASET_NAME, ds1.column_by_name('Empid').identifier)
        result = self.compute_lens_result(ds1, command)
        self.assertTrue(result.is_success)
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEqual(len(ds.columns), 4)
        self.assertEqual(ds.row_count, 2)
        names = set()
        empids = set()
        for row in ds.fetch_rows():
            empids.add(int(row.values[0]))
            names.add(row.values[1])
        self.assertTrue(1 in empids)
        self.assertTrue('Alice' in names or 'Bob' in names)
        self.assertFalse('Alice' in names and 'Bob' in names)
        self.assertTrue('Carla' in names)
        # Test error case and command text
        with self.assertRaises(ValueError):
            command = cmd.mimir_key_repair('MY DS', 'MY COL')
            result = self.compute_lens_result(ds, command)

    def test_missing_value_lens(self):
        """Test MISSING_VALUE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        command = cmd.mimir_missing_value(
            DATASET_NAME,
            columns=[{'column': ds.column_by_name('AGE').identifier}]
        )
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        for row in rows:
            self.assertIsNotNone(row.values[1])
        self.assertNotEqual(rows[2].values[ds.column_index('Age')], '')
        # MISSING VALUE Lens with value constraint
        command = cmd.mimir_missing_value(
            DATASET_NAME,
            columns=[{
                'column': ds.column_by_name('AGE').identifier,
                'constraint': '> 30'
            }],
        )
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        for row in rows:
            self.assertIsNotNone(row.values[1])
        print(rows[2].values)
        # we shouldn't be imputing a value lower than the minimum value in the dataset
        self.assertTrue(rows[2].values[ds.column_index('Age')] >= 23)

    def test_missing_key_lens(self):
        """Test MISSING_KEY lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        age_col = ds.column_by_name('Age').identifier
        command=cmd.mimir_missing_key(DATASET_NAME, age_col)
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEqual(len(ds.columns), 3)
        rows = ds.fetch_rows()
        # Depending on implementation this could be either 22 or 24, as there are two rows
        # with missing values for the key column.  Currently, Mimir discards such rows, but
        # if this suddenly turns into a 24, that's not incorrect either.
        self.assertEqual(len(rows), 22)
        command = cmd.mimir_missing_key(
            DATASET_NAME,
            ds.column_by_name('Salary').identifier
        )
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEqual(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEqual(len(rows), 31)

    def test_picker_lens(self):
        """Test PICKER lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        f_handle = self.filestore.upload_file(PICKER_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        command = cmd.mimir_picker(DATASET_NAME, [
            {'pickFrom': ds.column_by_name('Age').identifier},
            {'pickFrom': ds.column_by_name('Salary').identifier}
        ])
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        result_ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in result_ds.columns]
        # print(columns)
        self.assertEqual(len(result_ds .columns), 3)
        self.assertTrue('AGE_1' in columns)
        # Pick another column, this time with custom name
        command=cmd.mimir_picker(DATASET_NAME, [
                {'pickFrom': ds.column_by_name('Age').identifier},
                {'pickFrom': ds.column_by_name('Salary').identifier}
            ],
            pick_as='My_Column'
        )
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        result_ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in result_ds.columns]
        self.assertEqual(len(result_ds.columns), 3)
        self.assertTrue('MY_COLUMN' in columns)

    def test_type_inference_lens(self):
        """Test TYPE INFERENCE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Infer type
        command = cmd.mimir_type_inference(DATASET_NAME, 0.6)
        result = self.compute_lens_result(ds, command)
        self.assertTrue(result.is_success)
        # Get dataset
        ds2 = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEqual(len(ds2.columns), 3)
        self.assertEqual(ds2.row_count, 7)
        ds1_rows = ds.fetch_rows()
        ds2_rows = ds2.fetch_rows()
        for i in range(ds2.row_count):
            self.assertEqual(ds1_rows[i].values, ds2_rows[i].values)


if __name__ == '__main__':
    unittest.main()
