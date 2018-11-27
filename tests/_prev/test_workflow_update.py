"""Test worktrail repository implementation that uses the file system for
storage.
"""

import os
import shutil
import unittest

from vizier.api import VizierWebService
from vizier.config import AppConfig, ExecEnv, FileServerConfig
from vizier.config import ENGINEENV_DEFAULT, ENGINEENV_MIMIR
from vizier.datastore.client import DatasetClient
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mimir import MimirDataStore
from vizier.filestore.base import DefaultFileServer
from vizier.workflow.base import DEFAULT_BRANCH
from vizier.workflow.command import PACKAGE_VIZUAL, PACKAGE_PLOT
from vizier.workflow.engine.viztrails import DefaultViztrailsEngine
from vizier.workflow.repository.fs import FileSystemViztrailRepository
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.workflow.command as cmd

DATASTORE_DIR = './env/ds'
FILESERVER_DIR = './env/fs'
VIZTRAILS_DIR = './env/vt'

CSV_FILE = './data/dataset.csv'
CHART_NAME = 'My Chart'
DS_NAME = 'people'


class TestWorkflowUpdates(unittest.TestCase):

    def setUp(self):
        """Create an empty work trails repository."""
        # Create fresh set of directories
        self.config = AppConfig()
        env = ExecEnv(
                FileServerConfig().from_dict({'directory': FILESERVER_DIR}),
                packages=[PACKAGE_VIZUAL, PACKAGE_PLOT]
            ).from_dict({'datastore': {'directory': DATASTORE_DIR}})
        self.ENGINE_ID = env.identifier
        self.config.envs[self.ENGINE_ID] = env
        self.config.fileserver = env.fileserver
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)
            os.mkdir(d)
        self.datastore = FileSystemDataStore(DATASTORE_DIR)
        self.fileserver = DefaultFileServer(FILESERVER_DIR)
        self.db = FileSystemViztrailRepository(
            VIZTRAILS_DIR,
            {env.identifier: env}
        )
        self.api = VizierWebService(
            self.db,
            self.datastore,
            self.fileserver,
            self.config
        )

    def tearDown(self):
        """Clean-up by dropping the MongoDB collection used by the engine.
        """
        # Delete directories
        for d in [DATASTORE_DIR, FILESERVER_DIR, VIZTRAILS_DIR]:
            if os.path.isdir(d):
                shutil.rmtree(d)

    def test_view_urls(self):
        """Ensure that the urls for workflow views get updated correctly when
        the workflow is modified."""
        f_handle = self.fileserver.upload_file(CSV_FILE)
        vt = self.db.create_viztrail(self.ENGINE_ID, {'name' : 'My Project'})
        #print '(1) CREATE DATASET'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.load_dataset(f_handle.identifier, DS_NAME)
        )
        wf = self.db.get_workflow(viztrail_id=vt.identifier)
        #print '(2) PLOT'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.create_plot(DS_NAME, CHART_NAME, series=[{'series_column': 2}])
        )
        url = self.api.get_workflow(vt.identifier, DEFAULT_BRANCH)['state']['charts'][0]['links'][0]['href']
        self.assertTrue('master/workflows/1/modules/1/views' in url)
        # print '(3) UPDATE CELL'
        self.db.append_workflow_module(
            viztrail_id=vt.identifier,
            command=cmd.update_cell(DS_NAME, 0, 0, '28')
        )
        url = self.api.get_workflow(vt.identifier, DEFAULT_BRANCH)['state']['charts'][0]['links'][0]['href']
        self.assertTrue('master/workflows/2/modules/2/views' in url)


if __name__ == '__main__':
    unittest.main()
