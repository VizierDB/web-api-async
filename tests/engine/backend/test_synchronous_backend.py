"""Test the execute method of the synchronous backend."""

import os
import shutil
import unittest

from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.backend.synchron import SynchronousTaskEngine
from vizier.engine.base import compute_context
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON, PYTHON_CODE
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_LOAD, VIZUAL_UPD_CELL
from vizier.engine.packages.vizual.processor import VizualTaskProcessor
from vizier.engine.project.base import ProjectHandle
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.engine.task.base import TaskHandle
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.objectstore.repository import OSViztrailRepository

import vizier.engine.packages.vizual.command as vizual
import vizier.engine.packages.pycell.command as pycell
import vizier.engine.packages.base as pckg


SERVER_DIR = './.tmp'
DATASTORES_DIR = SERVER_DIR + '/ds'
FILESTORES_DIR = SERVER_DIR + '/fs'
VIZTRAILS_DIR = SERVER_DIR + '/vt'
CSV_FILE = './tests/engine/backend/.files/dataset.csv'

DATASET_NAME = 'people'
SECOND_DATASET_NAME = 'my_people'

CREATE_DATASET_PY = """
ds = vizierdb.new_dataset()
ds.insert_column('Name')
ds.insert_column('Age')
ds.insert_row(['Alice', 23])
ds.insert_row(['Bob', 34])
ds = vizierdb.create_dataset('""" + SECOND_DATASET_NAME + """', ds)
for row in ds.rows:
    print(row.get_value('Name'))
"""


class TestSynchronousTaskEngine(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default vizier processor for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        vizual = VizualTaskProcessor(api=DefaultVizualApi())
        pycell = PyCellTaskProcessor()
        projects = CommonProjectCache(
            datastores=FileSystemDatastoreFactory(DATASTORES_DIR),
            filestores=FileSystemFilestoreFactory(FILESTORES_DIR),
            viztrails=OSViztrailRepository(base_path=VIZTRAILS_DIR)
        )
        self.PROJECT_ID = projects.create_project().identifier
        self.backend = SynchronousTaskEngine(
            commands={
                PACKAGE_PYTHON: {PYTHON_CODE: pycell},
                PACKAGE_VIZUAL: {
                    VIZUAL_LOAD: vizual,
                    VIZUAL_UPD_CELL: vizual
                }
            },
            projects=projects
        )

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_can_execute(self):
        """Test the can execute method with different commands."""
        self.assertTrue(
            self.backend.can_execute(
                vizual.load_dataset(
                    dataset_name=DATASET_NAME,
                    file={pckg.FILE_ID: '000'},
                    validate=True
                )
            )
        )
        self.assertTrue(
            self.backend.can_execute(
                vizual.update_cell(
                    dataset_name=DATASET_NAME,
                    column=1,
                    row=0,
                    value=9,
                    validate=True
                )
            )
        )
        self.assertTrue(
            self.backend.can_execute(
                pycell.python_cell(
                    source=CREATE_DATASET_PY,
                    validate=True
                )
            )
        )
        self.assertFalse(
            self.backend.can_execute(
                vizual.insert_row(
                    dataset_name=DATASET_NAME,
                    position=1,
                    validate=True
                )
            )
        )
        self.assertFalse(
            self.backend.can_execute(
                vizual.drop_dataset(
                    dataset_name=DATASET_NAME,
                    validate=True
                )
            )
        )

    def test_execute(self):
        """Test executing a sequence of supported commands."""
        context = dict()
        fh = self.backend.projects.get_project(self.PROJECT_ID).filestore.upload_file(CSV_FILE)
        cmd = vizual.load_dataset(
            dataset_name=DATASET_NAME,
            file={pckg.FILE_ID: fh.identifier},
            validate=True
        )
        result = self.backend.execute(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID
            ),
            command=cmd,
            artifacts=context
        )
        self.assertTrue(result.is_success)
        context = result.provenance.get_database_state(prev_state=dict())
        cmd = vizual.update_cell(
            dataset_name=DATASET_NAME,
            column=1,
            row=0,
            value=9,
            validate=True
        )
        result = self.backend.execute(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID
            ),
            command=cmd,
            artifacts=context
        )
        self.assertTrue(result.is_success)
        prev_context = context
        context = result.provenance.get_database_state(prev_state=context)
        self.assertNotEqual(prev_context[DATASET_NAME], context[DATASET_NAME])
        cmd = pycell.python_cell(
            source=CREATE_DATASET_PY,
            validate=True
        )
        result = self.backend.execute(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID
            ),
            command=cmd,
            artifacts=context
        )
        if not result.is_success:
            print(result.outputs)
        self.assertTrue(result.is_success)
        prev_context = context
        context = result.provenance.get_database_state(prev_state=context)
        self.assertTrue(SECOND_DATASET_NAME in context)
        self.assertEqual(prev_context[DATASET_NAME], context[DATASET_NAME])
        cmd = vizual.update_cell(
            dataset_name=SECOND_DATASET_NAME,
            column=1,
            row=0,
            value=9,
            validate=True
        )
        result = self.backend.execute(
            task=TaskHandle(
                task_id='000',
                project_id=self.PROJECT_ID
            ),
            command=cmd,
            artifacts=context
        )
        self.assertTrue(result.is_success)
        prev_context = context
        context = result.provenance.get_database_state(prev_state=context)
        self.assertEqual(prev_context[DATASET_NAME], context[DATASET_NAME])
        self.assertNotEqual(prev_context[SECOND_DATASET_NAME], context[SECOND_DATASET_NAME])

    def test_execute_unsupported_command(self):
        """Test executing commands that are not supported for synchronous
        execution.
        """
        with self.assertRaises(ValueError):
            self.backend.execute(
                task=TaskHandle(
                    task_id='000',
                    project_id=self.PROJECT_ID
                ),
                command=vizual.insert_row(
                    dataset_name=DATASET_NAME,
                    position=1,
                    validate=True
                ),
                artifacts=dict()
            )

        with self.assertRaises(ValueError):
            self.backend.execute(
                task=TaskHandle(
                    task_id='000',
                    project_id=self.PROJECT_ID
                ),
                command=vizual.drop_dataset(
                    dataset_name=DATASET_NAME,
                    validate=True
                ),
                artifacts=dict()
            )


if __name__ == '__main__':
    unittest.main()
