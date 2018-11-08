"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

import vistrails.packages.mimir.init as mimir

from vizier.config import ExecEnv, FileServerConfig
from vizier.config import ENGINEENV_DEFAULT, ENGINEENV_MIMIR
from vizier.datastore.client import DatasetClient
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.command import PACKAGE_VIZUAL, PACKAGE_PYTHON, PACKAGE_MIMIR
from vizier.workflow.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.workflow.command as cmd

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
VIZTRAILS_DIR = './env/vt'

CSV_FILE = './data/mimir/cureSource.csv'

DS_NAME = 'shipments'

PYTHON_SCRIPT = """
# Get object for dataset with given name.
ds = vizierdb.get_dataset('shipments')

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
vizierdb.update_dataset('shipments', ds)
"""

class TestUnicodeHandling(unittest.TestCase):

    def tearDown(self):
        """Clean-up by dropping the MongoDB colelction used by the engine.
        """
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def set_up(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)

    def set_up_default(self):
        """Setup configuration using default Vizual engine."""
        env = ExecEnv(
                FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
                packages=[PACKAGE_VIZUAL, PACKAGE_PYTHON]
            ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
        self.ENGINE_ID = env.identifier
        self.set_up()
        self.datastore = FileSystemDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIR,
            {env.identifier: env}
        )

    def set_up_mimir(self):
        """Setup configuration using Mimir engine."""
        env = ExecEnv(
                FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
                identifier=ENGINEENV_MIMIR,
                packages=[PACKAGE_VIZUAL, PACKAGE_PYTHON, PACKAGE_MIMIR]
            ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
        self.ENGINE_ID = env.identifier
        self.set_up()
        self.datastore = MimirDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIR,
            {env.identifier: env}
        )

    def test_vt_default(self):
        """Run workflow with default configuration."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        self.set_up_default()
        self.run_workflow()

    def test_vt_mimir(self):
        """Run workflows for Mimir configurations."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        self.set_up_mimir()
        self.run_workflow()
        mimir.finalize()

    def run_workflow(self):
        """Test functionality to execute a Python script that creates a dataset
        containing unicode characters."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        # LOAD DATASET
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        # RUN Python Script
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(PYTHON_SCRIPT)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        if wf.has_error:
            print wf.modules[-1].stderr
        self.assertFalse(wf.has_error)
        #print wf.modules[-1].stdout[0]['data']
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        names = set(c.name.upper().replace('_', ' ') for c in ds.columns)
        self.assertTrue(len(names), 4)
        for name in ['DATE', 'IMO CODE', 'PORT OF DEPARTURE', 'PLACE OF RECEIPT']:
            self.assertTrue(name in names)


if __name__ == '__main__':
    unittest.main()
