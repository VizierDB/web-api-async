"""Test the execute method of the multiprocess backend when updating a
workflow by inserting, deleting and replacing modules.
"""

import os
import shutil
import time
import unittest

from vizier.api.client.command.pycell import python_cell
from vizier.api.client.command.vizual import load_dataset
from vizier.config import AppConfig

import vizier.engine.packages.base as pckg


SERVER_DIR = './.vizierdb'
CONFIG_FILE = './.files/config.yaml'
CSV_FILE = './.files/dataset.csv'

DATASET_NAME = 'people'

PY_ADD_ONE = """ds = vizierdb.get_dataset('""" + DATASET_NAME + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 1)
vizierdb.update_dataset('""" + DATASET_NAME + """', ds)
"""


class TestDefaultProjectEngine(unittest.TestCase):

    def setUp(self):
        """Create a default project engine from the default configuration."""
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        self.engine = AppConfig(configuration_file=CONFIG_FILE).get_api_engine()
        self.engine.init()

    def tearDown(self):
        """Clean-up by dropping the default vizier base directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def assert_value_is(self, project, value):
        """Assert value of the updated dataset cell."""
        wf = project.viztrail.default_branch.head
        ds = project.datastore.get_dataset(wf.modules[-1].datasets[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        self.assertEquals(rows[0].values[1], value)


    def create_workflow(self, project, count):
        """Create a completed workflow by loading the data file and updating the
        age value of the first row ten times.
        """
        branch_id = project.viztrail.default_branch.identifier
        fh = project.filestore.upload_file(CSV_FILE)
        cmd = load_dataset(
            dataset_name=DATASET_NAME,
            file_id={pckg.FILE_ID: fh.identifier}
        )
        project.append_workflow_module(branch_id=branch_id, command=cmd)
        for i in range(count):
            cmd = command=python_cell(PY_ADD_ONE)
            project.append_workflow_module(branch_id=branch_id, command=cmd)

    def test_create_and_delete(self):
        """Test creating projects, runnung workflows and reloading the engine."""
        ph1 = self.engine.create_project()
        ph2 = self.engine.create_project(properties={'name': 'My Project'})
        self.create_workflow(ph1, 10)
        self.create_workflow(ph2, 20)
        while ph1.viztrail.default_branch.head.is_active or ph2.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_value_is(ph1, 33)
        self.assert_value_is(ph2, 43)
        self.engine.delete_project(ph2.identifier)
        self.assertIsNone(self.engine.get_project(ph2.identifier))
        # Reload the engine
        engine = AppConfig(configuration_file=CONFIG_FILE).get_api_engine()
        engine.init()
        ph1 = engine.get_project(ph1.identifier)
        ph2 = engine.get_project(ph2.identifier)
        self.assert_value_is(ph1, 33)
        self.assertIsNone(ph2)
        self.assertEquals(len(engine.list_projects()), 1)

    def test_create_and_reload(self):
        """Test creating projects, runnung workflows and reloading the engine."""
        ph1 = self.engine.create_project()
        ph2 = self.engine.create_project(properties={'name': 'My Project'})
        self.assertIsNone(ph1.name)
        self.assertEquals(ph2.name, 'My Project')
        self.assertEquals(len(self.engine.list_projects()), 2)
        self.create_workflow(self.engine.get_project(ph1.identifier), 10)
        self.create_workflow(ph2, 20)
        ph1 = self.engine.get_project(ph1.identifier)
        self.assertEquals(len(ph1.viztrail.default_branch.head.modules), 11)
        self.assertEquals(len(ph2.viztrail.default_branch.head.modules), 21)
        while ph1.viztrail.default_branch.head.is_active or ph2.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_value_is(ph1, 33)
        self.assert_value_is(ph2, 43)
        # Reload the engine
        engine = AppConfig(configuration_file=CONFIG_FILE).get_api_engine()
        engine.init()
        self.assertEquals(len(engine.list_projects()), 2)
        ph1 = engine.get_project(ph1.identifier)
        ph2 = engine.get_project(ph2.identifier)
        self.assert_value_is(ph1, 33)
        self.assert_value_is(ph2, 43)
        wf1 = ph1.viztrail.default_branch.head
        self.assertFalse(wf1.is_active)
        self.assertEquals(len(wf1.modules), 11)
        for module in wf1.modules:
            self.assertTrue(module.is_success)
        wf2 = ph2.viztrail.default_branch.head
        self.assertFalse(wf2.is_active)
        self.assertEquals(len(wf2.modules), 21)
        for module in wf2.modules:
            self.assertTrue(module.is_success)
        for i in range(5):
            cmd = command=python_cell(PY_ADD_ONE)
            ph1.append_workflow_module(branch_id=wf1.branch_id, command=cmd)
        while ph1.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        # Reload the engine
        engine = AppConfig(configuration_file=CONFIG_FILE).get_api_engine()
        engine.init()
        ph1 = engine.get_project(ph1.identifier)
        self.assert_value_is(ph1, 38)
        wf1 = ph1.viztrail.default_branch.head
        for module in wf1.modules:
            self.assertTrue(module.is_success)
        self.assertFalse(wf1.is_active)
        self.assertEquals(len(wf1.modules), 16)
        ph2 = engine.get_project(ph2.identifier)
        self.assert_value_is(ph2, 43)
        wf2 = ph2.viztrail.default_branch.head
        self.assertFalse(wf2.is_active)
        self.assertEquals(len(wf2.modules), 21)
        for module in wf2.modules:
            self.assertTrue(module.is_success)


if __name__ == '__main__':
    unittest.main()
