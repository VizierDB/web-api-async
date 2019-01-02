"""Test the execute method of the synchronous backend."""

import os
import shutil
import time
import unittest

from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.backend.cache import ContextCache
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.controller import WorkflowController
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON, PYTHON_CODE
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_LOAD, VIZUAL_UPD_CELL
from vizier.engine.packages.vizual.processor import VizualTaskProcessor
from vizier.engine.project.base import ProjectHandle
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.engine.task.base import TaskHandle
from vizier.engine.task.processor import TaskProcessor
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.driver.objectstore.repository import OSViztrailRepository
from vizier.viztrail.command import ModuleCommand

import vizier.api.client.command.vizual as vizual
import vizier.api.client.command.pycell as pycell
import vizier.engine.packages.base as pckg



SERVER_DIR = './.tmp'
DATASTORES_DIR = SERVER_DIR + '/ds'
FILESTORES_DIR = SERVER_DIR + '/fs'
VIZTRAILS_DIR = SERVER_DIR + '/vt'
CSV_FILE = './.files/dataset.csv'

PROJECT_ID = '111'


class FakeTaskProcessor(TaskProcessor):
    def __init__(self):
        pass


class FakeWorkflowController(WorkflowController):
    def __init__(self):
        self.state = None
        self.outputs = None
        self.task_id = None

    def set_error(self, task_id, finished_at=None, outputs=None):
        self.task_id = task_id
        self.outputs = outputs
        self.state = 'ERROR'

    def set_success(self, task_id, finished_at=None, datasets=None, outputs=None, provenance=None):
        self.task_id = task_id
        self.outputs = outputs
        self.state = 'SUCCESS'


class TestMultiprocessBackend(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default vizier processor for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        projects = CommonProjectCache(
            datastores=FileSystemDatastoreFactory(DATASTORES_DIR),
            filestores=FileSystemFilestoreFactory(FILESTORES_DIR),
            viztrails=OSViztrailRepository(base_path=VIZTRAILS_DIR)
        )
        self.PROJECT_ID = projects.create_project().identifier
        self.backend = MultiProcessBackend(
            processors={
                PACKAGE_PYTHON: PyCellTaskProcessor(),
                PACKAGE_VIZUAL:  VizualTaskProcessor(api=DefaultVizualApi()),
                'error': FakeTaskProcessor()
            },
            projects=projects
        )

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_cancel(self):
        """Test executing a sequence of supported commands."""
        context = dict()
        cmd = pycell.python_cell(
            source='import time\ntime.sleep(5)',
            validate=True
        )
        controller = FakeWorkflowController()
        self.backend.execute_async(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID,
                controller=controller
            ),
            command=cmd,
            context=context
        )
        time.sleep(1)
        self.backend.cancel_task('000')
        time.sleep(5)
        self.assertIsNone(controller.task_id)
        self.assertIsNone(controller.state)

    def test_error(self):
        """Test executing a command with processor that raises an exception
        instead of returning an execution result.
        """
        context = dict()
        cmd = ModuleCommand(package_id='error', command_id='error')
        controller = FakeWorkflowController()
        self.backend.execute_async(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID,
                controller=controller
            ),
            command=cmd,
            context=context
        )
        time.sleep(2)
        self.assertEquals(controller.task_id, '000')
        self.assertEquals(controller.state, 'ERROR')
        self.assertEquals(len(controller.outputs.stdout), 0)
        self.assertEquals(controller.outputs.stderr[0].value, 'NotImplementedError:()')

    def test_execute(self):
        """Test executing a sequence of supported commands."""
        context = dict()
        cmd = pycell.python_cell(
            source='print 2+2',
            validate=True
        )
        controller = FakeWorkflowController()
        self.backend.execute_async(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID,
                controller=controller
            ),
            command=cmd,
            context=context
        )
        time.sleep(3)
        self.assertEquals(controller.task_id, '000')
        self.assertEquals(controller.state, 'SUCCESS')
        self.assertEquals(controller.outputs.stdout[0].value, '4')


if __name__ == '__main__':
    unittest.main()
