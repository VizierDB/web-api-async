"""Test the execute method of the synchronous backend."""

import os
import shutil
import time
import unittest
from datetime import datetime
from typing import Dict

from vizier.core.timestamp import get_current_time
from vizier.datastore.artifact import ArtifactDescriptor
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.controller import WorkflowController
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON
from vizier.engine.packages.pycell.base import PACKAGE_DATA
from vizier.engine.packages.pycell.processor.base import PyCellTaskProcessor
from vizier.engine.packages.vizual.api.mimir import MimirVizualApi
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL
from vizier.engine.packages.vizual.processor import VizualTaskProcessor
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.engine.task.base import TaskHandle
from vizier.engine.task.processor import TaskProcessor, ExecResult
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.objectstore.repository import OSViztrailRepository
from vizier.viztrail.command import ModuleCommand
from vizier.viztrail.module.output import ModuleOutputs

import vizier.engine.packages.pycell.command as pycell



SERVER_DIR = './.tmp'
DATASTORES_DIR = SERVER_DIR + '/ds'
FILESTORES_DIR = SERVER_DIR + '/fs'
VIZTRAILS_DIR = SERVER_DIR + '/vt'
CSV_FILE = './tests/engine/backend/.files/dataset.csv'

PROJECT_ID = '111'


class FakeTaskProcessor(TaskProcessor):
    def __init__(self):
        pass


class FakeWorkflowController(WorkflowController):
    def __init__(self):
        self.state = None
        self.outputs = None
        self.task_id = None

    def set_error(self, 
            task_id: str, 
            finished_at: datetime = None, 
            outputs: ModuleOutputs = None
        ):
        self.task_id = task_id
        self.outputs = outputs
        self.state = 'ERROR'

    def set_success(self, 
            task_id: str, 
            finished_at: datetime = get_current_time(),
            result: ExecResult = ExecResult()
        ):
        self.task_id = task_id
        self.outputs = result.outputs
        self.state = 'SUCCESS'

    def set_running(self):
        pass


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
        vtp = VizualTaskProcessor(api=MimirVizualApi())
        self.backend = MultiProcessBackend(
            processors={
                PACKAGE_PYTHON: PyCellTaskProcessor(),
                PACKAGE_VIZUAL: vtp,
                PACKAGE_DATA: vtp,
                'error': FakeTaskProcessor()
            },
            projects=projects
        )

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_cancel(self) -> None:
        """Test executing a sequence of supported commands."""
        context: Dict[str,ArtifactDescriptor] = dict()
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
            artifacts=context
        )
        time.sleep(1)
        self.backend.cancel_task('000')
        time.sleep(5)
        self.assertIsNone(controller.task_id)
        self.assertIsNone(controller.state)

    def test_error(self) -> None:
        """Test executing a command with processor that raises an exception
        instead of returning an execution result.
        """
        context: Dict[str, ArtifactDescriptor] = dict()
        cmd = ModuleCommand(
            package_id='error', 
            command_id='error',
            arguments=[],
            packages=None
        )
        controller = FakeWorkflowController()
        self.backend.execute_async(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID,
                controller=controller
            ),
            command=cmd,
            artifacts=context
        )
        time.sleep(2)
        self.assertEqual(controller.task_id, '000')
        self.assertEqual(controller.state, 'ERROR')
        self.assertEqual(len(controller.outputs.stdout), 0)
        self.assertNotEqual(len(controller.outputs.stderr), 0)

    def test_execute(self):
        """Test executing a sequence of supported commands."""
        context = dict()
        cmd = pycell.python_cell(
            source='print(2+2)',
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
            artifacts=context
        )
        time.sleep(3)
        self.assertEqual(controller.task_id, '000')
        self.assertEqual(controller.state, 'SUCCESS')
        self.assertEqual(controller.outputs.stdout[0].value, '4')


if __name__ == '__main__':
    unittest.main()
