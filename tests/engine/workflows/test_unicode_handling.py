"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

from vizier.datastore.fs.base import FileSystemDatastore
from vizier.datastore.mimir.store import MimirDatastore
from vizier.engine.packages.pycell.command import python_cell
from vizier.engine.task.base import TaskContext
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.filestore.fs.base import FileSystemFilestore


SERVER_DIR = '.tmp'
DATASTORE_DIR = os.path.join(SERVER_DIR, 'ds')
FILESERVER_DIR = os.path.join(SERVER_DIR, 'fs')
VIZTRAILS_DIR = os.path.join(SERVER_DIR, 'vt')

CSV_FILE = './.files/cureSource.csv'

DATASET_NAME = 'shipments'

PYTHON_SCRIPT = """
# Get object for dataset with given name.
ds = vizierdb.get_dataset('""" + DATASET_NAME + """')

projection = [ "IMO CODE", "PORT OF DEPARTURE", "DATE", "PLACE OF RECEIPT"]

# Iterate over list of dataset columns and print column name
col_index = 0
for col in list(ds.columns):
    print col.name
    if col.name.upper().replace('_', ' ') not in projection:
        ds.delete_column(col_index)
    else:
        col_index += 1
# Update existing dataset with given name.
vizierdb.update_dataset('""" + DATASET_NAME + """', ds)
"""

class TestUnicodeHandling(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.mkdir(SERVER_DIR)
        self.filestore = FileSystemFilestore(FILESERVER_DIR)

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        # Delete directories
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_default_config(self):
        """Run workflow with default configuration."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        self.run_workflow(FileSystemDatastore(DATASTORE_DIR))

    def test_mimir_config(self):
        """Run workflows for Mimir configurations."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        import vizier.mimir as mimir
        mimir.initialize()
        self.run_workflow(MimirDatastore(DATASTORE_DIR))
        mimir.finalize()

    def run_workflow(self, datastore):
        """Test functionality to execute a Python script that creates a dataset
        containing unicode characters."""
        f_handle = self.filestore.upload_file(CSV_FILE)
        ds = datastore.load_dataset(f_handle)
        # RUN Python Script
        cmd = python_cell(
            source=PYTHON_SCRIPT,
            validate=True
        )
        result = PyCellTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                datastore=datastore,
                filestore=self.filestore,
                datasets={DATASET_NAME: ds.identifier}
            )
        )
        self.assertTrue(result.is_success)
        #print wf.modules[-1].stdout[0]['data']
        ds = datastore.get_dataset(result.provenance.write[DATASET_NAME].identifier)
        names = set(c.name.upper().replace('_', ' ') for c in ds.columns)
        self.assertTrue(len(names), 4)
        for name in ['DATE', 'IMO CODE', 'PORT OF DEPARTURE', 'PLACE OF RECEIPT']:
            self.assertTrue(name in names)


if __name__ == '__main__':
    unittest.main()
