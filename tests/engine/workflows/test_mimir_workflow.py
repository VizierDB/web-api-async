"""Test workflow using the Mimir backend."""

import os
import shutil
import unittest
import time

from vizier.api.webservice.base import get_engine
from vizier.config.app import AppConfig
from vizier.config.base import MIMIR_ENGINE
from vizier.engine.packages.pycell.command import python_cell
from vizier.engine.packages.vizual.command import load_dataset

import vizier.mimir as mimir
import vizier.config.app as app
import vizier.engine.packages.base as pckg


SERVER_DIR = './.tmp'
PACKAGES_DIR = './.files/packages'
PROCESSORS_DIR = './.files/processors/mimir'
#PROCESSORS_DIR = './.files/processors'

CSV_FILE = './.files/people.csv'

DATASET_PEOPLE = 'people'
DATASET_FRIENDS = 'friends'

CREATE_DATASET_PY = """
ds = vizierdb.new_dataset()
ds.insert_column('Name')
ds.insert_column('Age')
ds.insert_row(['Yonder', 23])
ds.insert_row(['Zoe', 34])
vizierdb.create_dataset('""" + DATASET_FRIENDS + """', ds)
"""

PY_ADD_ONE = """ds = vizierdb.get_dataset('""" + DATASET_PEOPLE + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 1)
vizierdb.update_dataset('""" + DATASET_PEOPLE + """', ds)
"""


class TestMimirBackendWorkflows(unittest.TestCase):

    def setUp(self):
        """Create engine for an empty repository."""
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        os.environ[app.VIZIERSERVER_ENGINE] = MIMIR_ENGINE
        os.environ[app.VIZIERENGINE_DATA_DIR] = SERVER_DIR
        os.environ[app.VIZIERSERVER_PACKAGE_PATH] = PACKAGES_DIR
        os.environ[app.VIZIERSERVER_PROCESSOR_PATH] = PROCESSORS_DIR
        self.engine = get_engine(AppConfig())

    def tearDown(self):
        """Remove server directory."""
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_workflow(self):
        """Run workflows for Mimir configurations."""
        # Create new work trail and retrieve the HEAD workflow of the default
        # branch
        project = self.engine.projects.create_project()
        branch_id = project.viztrail.default_branch.identifier
        fh = project.filestore.upload_file(CSV_FILE)
        cmd = load_dataset(
            dataset_name=DATASET_PEOPLE,
            file={
                pckg.FILE_ID: fh.identifier,
                pckg.FILE_NAME: os.path.basename(CSV_FILE)
            }
        )
        self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=branch_id,
            command=cmd
        )
        cmd = python_cell(PY_ADD_ONE)
        self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=branch_id,
            command=cmd
        )
        wf = project.viztrail.default_branch.head
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        for m in wf.modules:
            self.assertTrue(m.is_success)
        cmd = python_cell(CREATE_DATASET_PY)
        self.engine.insert_workflow_module(
            project_id=project.identifier,
            branch_id=branch_id,
            before_module_id=wf.modules[0].identifier,
            command=cmd
        )
        wf = project.viztrail.default_branch.head
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        for m in wf.modules:
            self.assertTrue(m.is_success)
        self.assertTrue(DATASET_FRIENDS in wf.modules[0].datasets)
        self.assertFalse(DATASET_PEOPLE in wf.modules[0].datasets)
        for m in wf.modules[1:]:
            self.assertTrue(DATASET_FRIENDS in m.datasets)
            self.assertTrue(DATASET_PEOPLE in m.datasets)
        ds = project.datastore.get_dataset(wf.modules[-1].datasets[DATASET_PEOPLE].identifier)
        rows = ds.fetch_rows()
        self.assertEqual(rows[0].values, ['Alice', 24])
        self.assertEqual(rows[1].values, ['Bob', 32])
        ds = project.datastore.get_dataset(wf.modules[-1].datasets[DATASET_FRIENDS].identifier)
        rows = ds.fetch_rows()
        self.assertEqual(rows[0].values, ['Yonder', 23])
        self.assertEqual(rows[1].values, ['Zoe', 34])


if __name__ == '__main__':
    unittest.main()
