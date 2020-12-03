"""Test vizier pycell processor using the default datastore and filestore
implementations.
"""

import os
import shutil
import unittest

from vizier.engine.packages.pycell.command import python_cell
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.task.base import TaskContext
from vizier.engine.packages.pycell.processor.base import PyCellTaskProcessor
from vizier.filestore.fs.base import FileSystemFilestore


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './tests/engine/packages/pycell/.files/dataset.csv'

DATASET_NAME = 'people'

CREATE_DATASET_PY = """
ds = vizierdb.new_dataset()
ds.insert_column('Name')
ds.insert_column('Age')
ds.insert_row(['Alice', 23])
ds.insert_row(['Bob', 34])
ds.save('people')
for row in ds.rows:
    print(row.get_value('Name'))
"""

PRINT_DATASET_PY = """
for row in vizierdb.get_dataset('people').rows:
    print(row.get_value('Name'))
"""

PRINT_UNKNOWN_DATASET_PY = """
for row in vizierdb.get_dataset('employees').rows:
    print(row.get_value('Name'))
"""

PRINT_UNKNOWN_DATASET_PY_WITH_TRY_CATCH = """
try:
    for row in vizierdb.get_dataset('employees').rows:
        print(row.get_value('Name'))
except ValueError as ex:
    print(str(ex))
"""

class TestDefaultPyCellProcessor(unittest.TestCase):

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
                filestore=self.filestore,
                project_id=6,
                artifacts={}
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertEqual(len(result.provenance.read), 0)
        self.assertEqual(len(result.provenance.write), 1)
        self.assertTrue('people' in result.provenance.write)
        self.assertIsNotNone(result.provenance.write['people'])
        self.assertEqual(len(result.outputs.stdout), 1)
        self.assertEqual(len(result.outputs.stderr), 0)
        self.assertEqual(result.outputs.stdout[0].value, 'Alice\nBob')

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
                project_id=6,
                datastore=self.datastore,
                filestore=self.filestore,
                artifacts={'people': ds}
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertEqual(len(result.provenance.read), 1)
        self.assertEqual(len(result.provenance.write), 0)
        self.assertTrue('people' in result.provenance.read)
        self.assertIsNotNone(result.provenance.read['people'])
        self.assertEqual(len(result.outputs.stdout), 1)
        self.assertEqual(len(result.outputs.stderr), 0)
        self.assertEqual(result.outputs.stdout[0].value, 'Alice\nBob')

    def test_simple_script(self):
        """Test running the simple python script."""
        cmd = python_cell(
            source='print(2+2)',
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=self.datastore,
                filestore=self.filestore,
                project_id=6,
                artifacts={}
            )
        )
        self.assertTrue(result.is_success)
        self.assertEqual(result.outputs.stdout[0].value, '4')

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
                project_id=6,
                artifacts={'people': ds}
            )
        )
        self.assertFalse(result.is_success)
        self.assertTrue(result.provenance.read is None)
        self.assertTrue(result.provenance.write is None)
        self.assertEqual(len(result.outputs.stdout), 0)
        self.assertEqual(len(result.outputs.stderr), 1)
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
                project_id=6,
                artifacts={'people': ds}
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertEqual(len(result.provenance.read), 1)
        self.assertEqual(len(result.provenance.write), 0)
        self.assertTrue('employees' in result.provenance.read)
        self.assertIsNone(result.provenance.read['employees'])
        self.assertEqual(len(result.outputs.stdout), 1)
        self.assertEqual(len(result.outputs.stderr), 0)


if __name__ == '__main__':
    unittest.main()
