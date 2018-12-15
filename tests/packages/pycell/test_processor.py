"""Test vizier pycell processor using the default datastore and filestore
implementations.
"""

import os
import shutil
import unittest

from vizier.api.client.command.pycell import python_cell
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.task.base import TaskContext
from vizier.engine.packages.pycell.client.base import VizierDBClient
from vizier.engine.packages.pycell.client.dataset import DatasetClient
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.filestore.fs.base import DefaultFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './.files/dataset.csv'

DATASET_NAME = 'people'

CREATE_DATASET_PY = """
ds = vizierdb.new_dataset()
ds.insert_column('Name')
ds.insert_column('Age')
ds.insert_row(['Alice', 23])
ds.insert_row(['Bob', 34])
ds = vizierdb.create_dataset('people', ds)
for row in ds.rows:
    print row.get_value('Name')
"""

PRINT_DATASET_PY = """
for row in vizierdb.get_dataset('people').rows:
    print row.get_value('Name')
"""

PRINT_UNKNOWN_DATASET_PY = """
for row in vizierdb.get_dataset('employees').rows:
    print row.get_value('Name')
"""

PRINT_UNKNOWN_DATASET_PY_WITH_TRY_CATCH = """
try:
    for row in vizierdb.get_dataset('employees').rows:
        print row.get_value('Name')
except ValueError as ex:
    print str(ex)
"""

class TestDefaultPyCellProcessor(unittest.TestCase):

    def setUp(self):
        """Create instances of the default datastore and filestore."""
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

    def test_create_dataset_script(self):
        """Test running a script that creates a new datasets."""
        cmd = python_cell(
            source=CREATE_DATASET_PY,
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertEquals(len(result.provenance.read), 0)
        self.assertEquals(len(result.provenance.write), 1)
        self.assertTrue('people' in result.provenance.write)
        self.assertIsNotNone(result.provenance.write['people'])
        self.assertEquals(len(result.outputs.stdout), 1)
        self.assertEquals(len(result.outputs.stderr), 0)
        self.assertEquals(result.outputs.stdout[0].value, 'Alice\nBob')

    def test_print_dataset_script(self):
        """Test running a script that prints rows in an existing datasets."""
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.datastore.load_dataset(fh)
        cmd = python_cell(
            source=PRINT_DATASET_PY,
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={'people': ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertEquals(len(result.provenance.read), 1)
        self.assertEquals(len(result.provenance.write), 0)
        self.assertTrue('people' in result.provenance.read)
        self.assertIsNotNone(result.provenance.read['people'])
        self.assertEquals(len(result.outputs.stdout), 1)
        self.assertEquals(len(result.outputs.stderr), 0)
        self.assertEquals(result.outputs.stdout[0].value, 'Alice\nBob')

    def test_simple_script(self):
        """Test running the simple python script."""
        cmd = python_cell(
            source='print 2+2',
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets=dict()
            )
        )
        self.assertTrue(result.is_success)
        self.assertEquals(result.outputs.stdout[0].value, '4')

    def test_unknown_dataset_script(self):
        """Test running a script that accesses an unknown datasets."""
        fh = self.filestore.upload_file(CSV_FILE)
        ds = self.datastore.load_dataset(fh)
        cmd = python_cell(
            source=PRINT_UNKNOWN_DATASET_PY,
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={'people': ds.identifier}
            )
        )
        self.assertFalse(result.is_success)
        self.assertIsNone(result.provenance.read)
        self.assertIsNone(result.provenance.write)
        self.assertEquals(len(result.outputs.stdout), 0)
        self.assertEquals(len(result.outputs.stderr), 1)
        # Running a similar script that catches the error schould be a success
        # and the access to the dataset should be recorded in the resulting
        # read provenance
        cmd = python_cell(
            source=PRINT_UNKNOWN_DATASET_PY_WITH_TRY_CATCH,
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                datasets={'people': ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertEquals(len(result.provenance.read), 1)
        self.assertEquals(len(result.provenance.write), 0)
        self.assertTrue('employees' in result.provenance.read)
        self.assertIsNone(result.provenance.read['employees'])
        self.assertEquals(len(result.outputs.stdout), 1)
        self.assertEquals(len(result.outputs.stderr), 0)


if __name__ == '__main__':
    unittest.main()
