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

CSV_FILE = './data/dataset.csv'
INCOMPLETE_CSV_FILE = './data/dataset_with_missing_values.csv'

DS_NAME = 'people'

CREATE_DATASET_PY = """
ds = vizierdb.new_dataset()
ds.insert_column('Name')
ds.insert_column('Age')
ds.insert_row(['Alice', 23])
ds.insert_row(['Bob', 34])
vizierdb.create_dataset('people', ds)
"""

PRINT_DATASET_PY = """
for row in vizierdb.get_dataset('people').rows:
    print row.get_value('Name')
"""

PRINT_UNKNOWN_DATASET_PY = """
for row in vizierdb.get_dataset('employees').rows:
    print row.get_value('Name')
"""

SET_VARIABLES_PY = """
ds = vizierdb.get_dataset('people')
name_new = 'Bobby'
name_2 = ds.rows[2].get_value('Name')
ds.rows[1].set_value('Name', name_new)
vizierdb.update_dataset('people', ds)
ds = vizierdb.get_dataset('people')
"""

SET_VARIABLES_ONLY_PY = """
name_new = 'Bobby'
name_2 = 'Bob'
"""

UPDATE_DATASET_PY = """
ds = vizierdb.get_dataset('people')
for row in ds.rows:
    row.set_value('Name', 'NoName')
vizierdb.update_dataset('people', ds)
"""

UPDATE_DATASET_WITH_FILTER_PY = """
ds = vizierdb.get_dataset('people')
for row in ds.rows:
    if row.get_value('Name') == name_2:
        row.set_value('Name', name_new)
vizierdb.update_dataset('people', ds)
"""

class TestWorkflows(unittest.TestCase):

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
        self.run_python_workflow()
        self.set_up_default()
        self.run_mixed_workflow()
        self.set_up_default()
        self.run_delete_modules()
        self.set_up_default()
        self.run_erroneous_workflow()
        self.set_up_default()
        self.run_update_datasets()

    def test_vt_mimir(self):
        """Run workflows for Mimir configurations."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        mimir.initialize()
        self.set_up_mimir()
        self.run_python_workflow()
        self.set_up_mimir()
        self.run_mixed_workflow()
        self.set_up_mimir()
        self.run_delete_modules()
        self.set_up_mimir()
        self.run_erroneous_workflow()
        mimir.finalize()

    def run_delete_modules(self):
        """Test deletion of modules."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        col_age = ds.column_by_name('Age')
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, col_age.identifier, 0, '28')
        )
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, col_age.identifier, 1, '42')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = DatasetClient(self.datastore.get_dataset(wf.modules[-1].datasets['people']))
        self.assertEquals(int(ds.rows[0].get_value('Age')), 28)
        self.assertEquals(int(ds.rows[1].get_value('Age')), 42)
        # DELETE UPDATE CELL
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[1].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        ds = DatasetClient(self.datastore.get_dataset(wf.modules[-1].datasets['people']))
        self.assertEquals(int(ds.rows[0].get_value('Age')), 23)
        # DELETE LOAD (will introduce error)
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[0].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        # DELETE last remaining module
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[0].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)

    def run_erroneous_workflow(self):
        """Test workflow that has errors."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        col_age = ds.column_by_name('Age')
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, col_age.identifier, 0, '28')
        )
        # This should create an error because of the invalid column name
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.rename_column(DS_NAME, col_age.identifier, '')
        )
        # This should not have any effect
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, col_age.identifier, 0, '29')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        # Make sure that all workflow modules have a non-negative identifier
        # and that they are all unique
        identifier = set()
        for m in wf.modules:
            self.assertTrue(m.identifier >= 0)
            self.assertTrue(not m.identifier in identifier)
            identifier.add(m.identifier)

    def run_mixed_workflow(self):
        """Test functionality to execute a workflow module."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        cmd_text = wf.modules[-1].command_text
        self.assertEquals(cmd_text, 'LOAD DATASET people FROM FILE dataset.csv')
        #print '(2) INSERT ROW'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.insert_row(DS_NAME, 1)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        cmd_text = wf.modules[-1].command_text
        self.assertEquals(cmd_text, 'INSERT ROW INTO people AT POSITION 1')
        #print '(3) Set name to Bobby and set variables'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(SET_VARIABLES_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        cmd_text = wf.modules[-1].command_text
        self.assertEquals(cmd_text, SET_VARIABLES_PY)
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        #print '(4) Set age to 28'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, ds.column_by_name('Age').identifier, 1, '28')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        cmd_text = wf.modules[-1].command_text
        self.assertEquals(cmd_text.upper(), 'UPDATE PEOPLE SET [AGE,1] = 28')
        ds = self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME])
        #print '(5) Change Alice to Bob'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, ds.column_by_name('Name').identifier, 0, 'Bob')
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        cmd_text = wf.modules[-1].command_text
        self.assertEquals(cmd_text.upper(), 'UPDATE PEOPLE SET [NAME,0] = \'BOB\'')
        #print '(6) UPDATE DATASET WITH FILTER'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(UPDATE_DATASET_WITH_FILTER_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        cmd_text = wf.modules[-1].command_text
        self.assertEquals(cmd_text, UPDATE_DATASET_WITH_FILTER_PY)
        self.assertFalse(wf.has_error)
        # Ensure that all names are Bobby
        ds = DatasetClient(self.datastore.get_dataset(wf.modules[-1].datasets[DS_NAME]))
        age = [23, 28, 32]
        for i in range(len(ds.rows)):
            row = ds.rows[i]
            self.assertEquals(row.get_value('Name'), 'Bobby')
            self.assertEquals(int(row.get_value('Age')), age[i])

    def run_python_workflow(self):
        """Test functionality to execute a workflow module."""
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(CREATE_DATASET_PY)
        )
        # from vizier.database.client import VizierDBClient\nv = VizierDBClient(__vizierdb__)
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        self.assertEquals(wf.version, 0)
        self.assertEquals(len(wf.modules), 1)
        self.assertTrue(len(wf.modules[0].stdout) == 0)
        self.assertTrue(len(wf.modules[0].stderr) == 0)
        self.assertEquals(len(wf.modules[0].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        #print '(2) PRINT DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(PRINT_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertEquals(wf.version, 1)
        self.assertEquals(len(wf.modules), 2)
        self.assertTrue(len(wf.modules[0].stdout) == 0)
        self.assertTrue(len(wf.modules[0].stderr) == 0)
        self.assertEquals(len(wf.modules[0].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        self.assertTrue(len(wf.modules[1].stdout) == 1)
        self.assertTrue(len(wf.modules[1].stderr) == 0)
        self.assertEquals(wf.modules[1].stdout[0]['data'], 'Alice\nBob')
        self.assertEquals(len(wf.modules[1].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[1].datasets)
        ds_id = wf.modules[1].datasets[DS_NAME]
        #print '(3) UPDATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(UPDATE_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.version, 2)
        self.assertEquals(len(wf.modules), 3)
        self.assertTrue(len(wf.modules[0].stdout) == 0)
        self.assertTrue(len(wf.modules[0].stderr) == 0)
        self.assertEquals(len(wf.modules[0].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        self.assertEquals(wf.modules[0].datasets[DS_NAME], ds_id)
        self.assertTrue(len(wf.modules[1].stdout) == 1)
        self.assertTrue(len(wf.modules[1].stderr) == 0)
        self.assertEquals(wf.modules[1].stdout[0]['data'], 'Alice\nBob')
        self.assertEquals(len(wf.modules[1].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[1].datasets)
        self.assertEquals(wf.modules[1].datasets[DS_NAME], ds_id)
        self.assertTrue(len(wf.modules[2].stdout) == 0)
        self.assertTrue(len(wf.modules[2].stderr) == 0)
        self.assertEquals(len(wf.modules[2].datasets), 1)
        self.assertTrue(DS_NAME in wf.modules[2].datasets)
        self.assertNotEquals(wf.modules[2].datasets[DS_NAME], ds_id)
        #print '(4) PRINT DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(PRINT_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertEquals(wf.version, 3)
        self.assertEquals(len(wf.modules), 4)
        self.assertEquals(wf.modules[1].stdout[0]['data'], 'Alice\nBob')
        self.assertTrue(len(wf.modules[3].stdout) == 1)
        self.assertTrue(len(wf.modules[3].stderr) == 0)
        self.assertEquals(wf.modules[3].stdout[0]['data'], 'NoName\nNoName')
        #print '(5) UPDATE DATASET WITH FILTER'
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[2].identifier,
            command=cmd.python_cell(UPDATE_DATASET_WITH_FILTER_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertTrue(wf.has_error)
        self.assertEquals(wf.version, 4)
        self.assertEquals(len(wf.modules), 4)
        # print '(6) INSERT SET VARIABLES BEFORE UPDATE'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.python_cell(SET_VARIABLES_ONLY_PY),
            before_id= wf.modules[2].identifier

        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[4].stdout[0]['data'], 'Alice\nBobby')
        #print '(7) INTRODUCE ERROR'
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[1].identifier,
            command=cmd.python_cell(PRINT_UNKNOWN_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        self.assertTrue(wf.has_error)
        # Ensure that the second module has output to stderr
        self.assertNotEquals(len( wf.modules[1].stderr), 0)
        # Ensure that the last two modules hav no output (either to STDOUT or
        # STDERR)
        for m in wf.modules[2:]:
            self.assertEquals(len(m.stdout), 0)
            self.assertEquals(len(m.stderr), 0)
        #print '(8) FIX ERROR'
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[1].identifier,
            command=cmd.python_cell(PRINT_DATASET_PY)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        prev_modules = modules
        modules = set()
        for m in wf.modules:
            self.assertNotEquals(m.identifier, -1)
            self.assertFalse(m.identifier in modules)
            modules.add(m.identifier)
        # Ensure that the identifier of previous modules did not change
        for id in prev_modules:
            self.assertTrue(id in modules)
        #print (9) DELETE MODULE UPDATE_DATASET_WITH_FILTER_PY
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[3].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[3].stdout[0]['data'], 'Alice\nBob')

    def run_update_datasets(self):
        """Test dropping and renaming of datasets."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertTrue(DS_NAME in wf.modules[-1].datasets)
        new_name = DS_NAME + '_renamed'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.rename_dataset(DS_NAME, new_name)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertTrue(DS_NAME in wf.modules[0].datasets)
        self.assertFalse(new_name in wf.modules[0].datasets)
        self.assertFalse(DS_NAME in wf.modules[-1].datasets)
        self.assertTrue(new_name in wf.modules[-1].datasets)
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.drop_dataset(new_name)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertFalse(new_name in wf.modules[-1].datasets)
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.drop_dataset(new_name)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertTrue(wf.has_error)
        # Delete the Drop Dataset that failed and replace the first drop with
        # a Python module that prints names
        self.db.delete_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[-1].identifier
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.db.replace_workflow_module(
            viztrail_id=vt.identifier,
            module_id=wf.modules[-1].identifier,
            command=cmd.python_cell(
"""
for row in vizierdb.get_dataset('""" + new_name + """').rows:
    print row.get_value('Name')
"""
            )
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        self.assertFalse(wf.has_error)
        self.assertEquals(wf.modules[-1].stdout[0]['data'], 'Alice\nBob')
        self.assertFalse(DS_NAME in wf.modules[-1].datasets)
        self.assertTrue(new_name in wf.modules[-1].datasets)


if __name__ == '__main__':
    unittest.main()
