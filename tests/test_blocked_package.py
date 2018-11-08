import csv
import os
import shutil
import sys
import time
import unittest

from vizier.config import ExecEnv, TestEnv, ENGINEENV_DEFAULT
from vizier.datastore.fs import FileSystemDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.command import python_cell, mimir_missing_value
from vizier.workflow.repository.fs import FileSystemViztrailRepository

from vizier.api import VizierWebService
from vizier.config import AppConfig

CONFIG_FILE = './data/api-config.yaml'
FILESERVER_DIR = './env/fs'
DATASTORE_DIRECTORY = './env/ds'
WORKTRAILS_DIRECTORY = './env/wt'

class TestWebServiceAPI(unittest.TestCase):

    def setUp(self):
        """Create an new Web Service API."""
        # Clear various directories
        for d in [WORKTRAILS_DIRECTORY, DATASTORE_DIRECTORY, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)
        # Setup datastore and API
        self.config = AppConfig(configuration_file=CONFIG_FILE)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        self.config.envs = {
            'default': TestEnv(),
            'blocked': self.config.envs[ENGINEENV_DEFAULT]
        }
        self.datastore = FileSystemDataStore(DATASTORE_DIRECTORY)
        self.api = VizierWebService(
            FileSystemViztrailRepository(
                WORKTRAILS_DIRECTORY,
                self.config.envs
            ),
            self.datastore,
            self.fileserver,
            self.config
        )

    def tearDown(self):
        """Clean-up by deleting created directories.
        """
        for d in [WORKTRAILS_DIRECTORY, DATASTORE_DIRECTORY, FILESERVER_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_workflows(self):
        """Test API calls to retrieve and manipulate workflows."""
        # Create a new project
        ph = self.api.create_project('default', {'name' : 'My Project'})
        wf = self.api.get_workflow(ph['id'], DEFAULT_BRANCH)
        self.api.append_module(ph['id'], DEFAULT_BRANCH, -1, python_cell('2+2'))
        wf = self.api.get_workflow(ph['id'], DEFAULT_BRANCH)
        #self.assertEquals(len(wf['modules']), 1)
        # Ensure exception is thrown if package is blocked
        ph = self.api.create_project('blocked', {'name' : 'My Project'})
        wf = self.api.get_workflow(ph['id'], DEFAULT_BRANCH)
        with self.assertRaises(ValueError):
            self.api.append_module(ph['id'], DEFAULT_BRANCH, -1, mimir_missing_value('2+2', 3))


if __name__ == '__main__':
    unittest.main()
