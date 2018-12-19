"""Test default implementation for vizual package task processor."""

import os
import shutil
import unittest

from vizier.core.loader import ClassLoader
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.task.base import TaskContext
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi, RESOURCE_DATASET
from vizier.engine.packages.vizual.processor import VizualTaskProcessor, PROPERTY_API
from vizier.filestore.fs.base import DefaultFilestore

import vizier.api.client.command.vizual as vizual
import vizier.engine.packages.base as pckg


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
        self.processor = VizualTaskProcessor(api=DefaultVizualApi())
        self.datastore=FileSystemDatastore(DATASTORE_DIR)
        self.filestore=DefaultFilestore(FILESTORE_DIR)

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_create_api_from_dictionary(self):
        """Test creating the processor instance with properties parameter
        instead of api.
        """
        processor = VizualTaskProcessor(
            properties={
                PROPERTY_API: ClassLoader.to_dict(
                    module_name='vizier.engine.packages.vizual.api.fs',
                    class_name='DefaultVizualApi'
                )
            }
        )
        fh = self.filestore.upload_file(CSV_FILE)
        cmd = vizual.load_dataset(
            dataset_name=DATASET_NAME,
            file={pckg.FILE_ID: fh.identifier},
            validate=True
        )
        result = processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore
            )
        )
        self.assertIsNotNone(result.provenance.write)
        self.assertTrue(DATASET_NAME in result.datasets)
        dataset_id = result.datasets[DATASET_NAME]
        self.assertEquals(result.provenance.write[DATASET_NAME], dataset_id)
        self.assertIsNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.resources)
        self.assertEquals(result.provenance.resources[RESOURCE_DATASET], dataset_id)

    def load_dataset(self):
        """Load a single dataset and return the resulting database state."""
        fh = self.filestore.upload_file(CSV_FILE)
        cmd = vizual.load_dataset(
            dataset_name=DATASET_NAME,
            file={pckg.FILE_ID: fh.identifier},
            validate=True
        )
        result = self.processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore
            )
        )
        return result.datasets

    def test_delete_column(self):
        """Test functionality to delete a column."""
        cmd = vizual.delete_column(
            dataset_name=DATASET_NAME,
            column=1,
            validate=True
        )
        self.validate_command(cmd)

    def test_delete_row(self):
        """Test functionality to delete a row."""
        cmd = vizual.delete_row(
            dataset_name=DATASET_NAME,
            row=1,
            validate=True
        )
        self.validate_command(cmd)

    def test_drop_dataset(self):
        """Test functionality to drop a dataset."""
        cmd = vizual.drop_dataset(
            dataset_name=DATASET_NAME,
            validate=True
        )
        datasets = self.load_dataset()
        result = self.processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets=datasets
            )
        )
        self.assertFalse(DATASET_NAME in result.datasets)
        self.assertFalse(DATASET_NAME in result.provenance.read)
        self.assertTrue(DATASET_NAME in result.provenance.delete)
        self.assertFalse(DATASET_NAME in result.provenance.write)

    def test_filter_columns(self):
        """Test projection of a dataset."""
        # Create a new dataset
        cmd = vizual.projection(
            dataset_name=DATASET_NAME,
            columns=[
                {'column': 1},
                {'column': 2, 'name': 'MyName'}
            ],
            validate=True
        )
        self.validate_command(cmd)

    def test_insert_column(self):
        """Test functionality to insert a columns."""
        cmd = vizual.insert_column(
            dataset_name=DATASET_NAME,
            position=1,
            name='My Col',
            validate=True
        )
        self.validate_command(cmd)

    def test_insert_row(self):
        """Test functionality to insert a row."""
        # Create a new dataset
        cmd = vizual.insert_row(
            dataset_name=DATASET_NAME,
            position=1,
            validate=True
        )
        self.validate_command(cmd)

    def test_load_dataset(self):
        """Test functionality to load a dataset."""
        # Create a new dataset
        fh = self.filestore.upload_file(CSV_FILE)
        cmd = vizual.load_dataset(
            dataset_name='ABC',
            file={pckg.FILE_ID: fh.identifier},
            validate=True
        )
        result = self.processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore
            )
        )
        self.assertIsNotNone(result.provenance.write)
        self.assertTrue('abc' in result.datasets)
        dataset_id = result.datasets['abc']
        self.assertEquals(result.provenance.write['abc'], dataset_id)
        self.assertIsNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.resources)
        self.assertEquals(result.provenance.resources[RESOURCE_DATASET], dataset_id)
        # Running load again will not change the dataset identifier
        result = self.processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                resources=result.provenance.resources
            )
        )
        self.assertEquals(result.datasets['abc'], dataset_id)
        self.assertEquals(result.provenance.write['abc'], dataset_id)
        self.assertEquals(result.provenance.resources[RESOURCE_DATASET], dataset_id)

    def test_move_column(self):
        """Test functionality to move a column."""
        cmd = vizual.move_column(
            dataset_name=DATASET_NAME,
            column=0,
            position=1,
            validate=True
        )
        self.validate_command(cmd)

    def test_move_row(self):
        """Test functionality to move a row."""
        cmd = vizual.move_row(
            dataset_name=DATASET_NAME,
            row=0,
            position=1,
            validate=True
        )
        self.validate_command(cmd)

    def test_rename_column(self):
        """Test functionality to rename a column."""
        cmd = vizual.rename_column(
            dataset_name=DATASET_NAME,
            column=1,
            name='The col',
            validate=True
        )
        self.validate_command(cmd)

    def test_rename_dataset(self):
        """Test functionality to rename a dataset."""
        cmd = vizual.rename_dataset(
            dataset_name=DATASET_NAME,
            new_name='XYZ',
            validate=True
        )
        datasets = self.load_dataset()
        result = self.processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets=datasets
            )
        )
        self.assertFalse(DATASET_NAME in result.datasets)
        self.assertTrue('xyz' in result.datasets)
        self.assertFalse(DATASET_NAME in result.provenance.read)
        self.assertTrue(DATASET_NAME in result.provenance.delete)
        self.assertFalse(DATASET_NAME in result.provenance.write)
        self.assertTrue('xyz' in result.provenance.write)

    def test_sort_dataset(self):
        """Test sorting a dataset."""
        cmd = vizual.sort_dataset(
            dataset_name=DATASET_NAME,
            columns=[
                {'column': 1, 'order': 'Z-A'},
                {'column': 2, 'order': 'A-Z'}
            ],
            validate=True
        )
        self.validate_command(cmd)

    def test_update_cell(self):
        """Test functionality to update a dataset cell."""
        # Create a new dataset
        cmd = vizual.update_cell(
            dataset_name=DATASET_NAME,
            column=1,
            row=0,
            value=9,
            validate=True
        )
        self.validate_command(cmd)

    def validate_command(self, cmd):
        """Validate execution of the given command."""
        datasets = self.load_dataset()
        dataset_id = datasets[DATASET_NAME]
        result = self.processor.compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets=datasets
            )
        )
        self.assertEquals(datasets[DATASET_NAME], dataset_id)
        self.assertNotEqual(result.datasets[DATASET_NAME], dataset_id)
        self.assertIsNotNone(result.provenance.read)
        self.assertEquals(result.provenance.read[DATASET_NAME], dataset_id)
        self.assertEquals(result.provenance.write[DATASET_NAME], result.datasets[DATASET_NAME])
        self.assertIsNotNone(result.provenance.write)
        with self.assertRaises(ValueError):
            result = self.processor.compute(
                command_id=cmd.command_id,
                arguments=cmd.arguments,
                context=TaskContext(
                    datastore=self.datastore,
                    filestore=self.filestore
                )
            )

if __name__ == '__main__':
    unittest.main()
