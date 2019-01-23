"""Test task processor for Mimir lenses."""

import os
import shutil
import unittest

from vizier.core.loader import ClassLoader
from vizier.engine.task.base import TaskContext

from vizier.datastore.mimir.store import MimirDatastore
from vizier.engine.packages.mimir.processor import MimirProcessor
from vizier.filestore.fs.base import FileSystemFilestore

import vizier.engine.packages.mimir.command as cmd
import vizier.engine.packages.base as pckg
import vizier.mimir as mimir


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'

CSV_FILE = './.files/dataset.csv'
GEO_FILE = './.files/geo.csv'
KEY_REPAIR_FILE = './.files/key_repair.csv'
INCOMPLETE_CSV_FILE = './.files/dataset_with_missing_values.csv'
PICKER_FILE = './.files/dataset_pick.csv'

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

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_domain_lens(self):
        """Test DOMAIN lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        col_age = ds.column_by_name('Age')
        command = cmd.mimir_domain(DATASET_NAME, col_age.identifier)
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        self.assertNotEquals(rows[2].values[ds.column_index('Age')], '')
        # Introduce an error. Make sure command formating is correct
        command = cmd.mimir_domain('MY DS', 'MY COL')
        with self.assertRaises(ValueError):
            result = self.processor.compute(
                command_id=command.command_id,
                arguments=command.arguments,
                context=TaskContext(
                    datastore=self.datastore,
                    filestore=self.filestore,
                    datasets={DATASET_NAME: ds.identifier}
                )
            )
        mimir.finalize()

    def test_geocode_lens(self):
        """Test GEOCODE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
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
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(columns), 6)
        self.assertTrue('LATITUDE' in columns)
        self.assertTrue('LONGITUDE' in columns)
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(columns), 8)
        self.assertTrue('LATITUDE_1' in columns)
        self.assertTrue('LONGITUDE_1' in columns)
        self.assertEquals(len(ds.columns), 8)
        mimir.finalize()

    def test_key_repair_lens(self):
        """Test KEY REPAIR lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(KEY_REPAIR_FILE)
        ds1 = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        command = cmd.mimir_key_repair(DATASET_NAME, ds1.column_by_name('Empid').identifier)
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds1.identifier}
            )
        )
        self.assertTrue(result.is_success)
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEquals(len(ds.columns), 4)
        self.assertEquals(ds.row_count, 3)
        names = set()
        empids = set()
        rowids = set()
        for row in ds.fetch_rows():
            rowids.add(row.identifier)
            empids.add(int(row.get_value('empid')))
            names.add(row.get_value('name'))
        self.assertTrue(1 in empids)
        self.assertTrue(2 in rowids)
        self.assertTrue('Alice' in names)
        self.assertTrue('Carla' in names)
        # Test error case and command text
        command = cmd.mimir_key_repair('MY DS', 'MY COL')
        with self.assertRaises(ValueError):
            self.processor.compute(
                command_id=command.command_id,
                arguments=command.arguments,
                context=TaskContext(
                    datastore=self.datastore,
                    filestore=self.filestore,
                    datasets={DATASET_NAME: ds.identifier}
                )
            )
        mimir.finalize()

    def test_missing_value_lens(self):
        """Test MISSING_VALUE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        command = cmd.mimir_missing_value(
            DATASET_NAME,
            columns=[{'column': ds.column_by_name('AGE').identifier}]
        )
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        for row in rows:
            self.assertIsNotNone(row.values[1])
        self.assertNotEquals(rows[2].values[ds.column_index('Age')], '')
        # MISSING VALUE Lens with value constraint
        command = cmd.mimir_missing_value(
            DATASET_NAME,
            columns=[{
                'column': ds.column_by_name('AGE').identifier,
                'constraint': '> 30'
            }],
        )
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        for row in rows:
            self.assertIsNotNone(row.values[1])
        self.assertTrue(rows[2].values[ds.column_index('Age')] > 30)
        mimir.finalize()

    def test_missing_key_lens(self):
        """Test MISSING_KEY lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        age_col = ds.column_by_name('Age').identifier
        command=cmd.mimir_missing_key(DATASET_NAME, age_col)
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEquals(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 24)
        command = cmd.mimir_missing_key(
            DATASET_NAME,
            ds.column_by_name('Salary').identifier
        )
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEquals(len(ds.columns), 3)
        rows = ds.fetch_rows()
        self.assertEquals(len(rows), 55)
        mimir.finalize()

    def test_picker_lens(self):
        """Test PICKER lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(PICKER_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        command = cmd.mimir_picker(DATASET_NAME, [
            {'pickFrom': ds.column_by_name('Age').identifier},
            {'pickFrom': ds.column_by_name('Salary').identifier}
        ])
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        print columns
        self.assertEquals(len(ds.columns), 5)
        self.assertTrue('PICK_ONE_AGE_SALARY' in columns)
        # Pick another column, this time with custom name
        command=cmd.mimir_picker(DATASET_NAME, [
                {'pickFrom': ds.column_by_name('Age').identifier},
                {'pickFrom': ds.column_by_name('Salary').identifier}
            ],
            pick_as='My_Column'
        )
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        self.assertEquals(len(ds.columns), 6)
        self.assertTrue('PICK_ONE_AGE_SALARY' in columns)
        self.assertTrue('MY_COLUMN' in columns)
        # Pick from a picked column
        command=cmd.mimir_picker(DATASET_NAME, [
                {'pickFrom': ds.column_by_name('Age').identifier},
                {'pickFrom': ds.column_by_name('PICK_ONE_AGE_SALARY').identifier}
            ],
            pick_as='My_Next_Column'
        )
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        columns = [c.name for c in ds.columns]
        self.assertTrue('MY_NEXT_COLUMN' in columns)
        mimir.finalize()

    def test_schema_matching_lens(self):
        """Test SCHEMA_MATCHING lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Missing Value Lens
        command = cmd.mimir_schema_matching(DATASET_NAME, [
                {'column': 'BDate', 'type': 'int'},
                {'column': 'PName', 'type': 'varchar'}
            ], 'new_' + DATASET_NAME
        )
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds = self.datastore.get_dataset(result.provenance.write['new_' + DATASET_NAME].identifier)
        self.assertEquals(len(ds.columns), 2)
        self.assertEquals(ds.row_count, 2)
        mimir.finalize()

    def test_type_inference_lens(self):
        """Test TYPE INFERENCE lens."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        f_handle = self.filestore.upload_file(INCOMPLETE_CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        # Infer type
        command = cmd.mimir_type_inference(DATASET_NAME, 0.6)
        result = self.processor.compute(
            command_id=command.command_id,
            arguments=command.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        # Get dataset
        ds2 = self.datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        self.assertEquals(len(ds2.columns), 3)
        self.assertEquals(ds2.row_count, 7)
        ds1_rows = ds.fetch_rows()
        ds2_rows = ds2.fetch_rows()
        for i in range(ds2.row_count):
            self.assertEquals(ds1_rows[i].values, ds2_rows[i].values)
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
