"""Test the execute method of the synchronous backend."""

import os
import shutil
import unittest

from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.backend.synchron import SynchronousBackend
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON, PYTHON_CODE
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL, VIZUAL_LOAD, VIZUAL_UPD_CELL
from vizier.engine.packages.vizual.processor import VizualTaskProcessor
from vizier.filestore.fs.base import DefaultFilestore

import vizier.client.command.vizual as vizual
import vizier.client.command.pycell as pycell
import vizier.engine.packages.base as pckg



SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
CSV_FILE = './.files/dataset.csv'

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
    print row.get_value('Name')
"""


class TestSynchronousBackend(unittest.TestCase):

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
        self.backend = SynchronousBackend(
            datastore=FileSystemDatastore(DATASTORE_DIR),
            filestore=DefaultFilestore(FILESTORE_DIR),
            commands={
                PACKAGE_PYTHON: {PYTHON_CODE: pycell},
                PACKAGE_VIZUAL: {
                    VIZUAL_LOAD: vizual,
                    VIZUAL_UPD_CELL: vizual
                }
            }
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
                    file_id={pckg.FILE_ID: '000'},
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
        fh = self.backend.filestore.upload_file(CSV_FILE)
        cmd = vizual.load_dataset(
            dataset_name=DATASET_NAME,
            file_id={pckg.FILE_ID: fh.identifier},
            validate=True
        )
        result = self.backend.execute(command=cmd, context=context)
        self.assertTrue(result.is_success)
        context = result.datasets
        cmd = vizual.update_cell(
            dataset_name=DATASET_NAME,
            column=1,
            row=0,
            value=9,
            validate=True
        )
        result = self.backend.execute(command=cmd, context=context)
        self.assertTrue(result.is_success)
        self.assertNotEqual(context[DATASET_NAME], result.datasets[DATASET_NAME])
        context = result.datasets
        cmd = pycell.python_cell(
            source=CREATE_DATASET_PY,
            validate=True
        )
        result = self.backend.execute(command=cmd, context=context)
        self.assertTrue(result.is_success)
        self.assertEquals(context[DATASET_NAME], result.datasets[DATASET_NAME])
        self.assertTrue(SECOND_DATASET_NAME in result.datasets)
        context = result.datasets
        cmd = vizual.update_cell(
            dataset_name=SECOND_DATASET_NAME,
            column=1,
            row=0,
            value=9,
            validate=True
        )
        result = self.backend.execute(command=cmd, context=context)
        self.assertTrue(result.is_success)
        self.assertEquals(context[DATASET_NAME], result.datasets[DATASET_NAME])
        self.assertNotEqual(context[SECOND_DATASET_NAME], result.datasets[SECOND_DATASET_NAME])

    def test_execute_unsupported_command(self):
        """Test executing commands that are not supported for synchronous
        execution.
        """
        with self.assertRaises(ValueError):
            self.backend.execute(
                command=vizual.insert_row(
                    dataset_name=DATASET_NAME,
                    position=1,
                    validate=True
                ),
                context=dict()
            )

        with self.assertRaises(ValueError):
            self.backend.execute(
                command=vizual.drop_dataset(
                    dataset_name=DATASET_NAME,
                    validate=True
                ),
                context=dict()
            )


if __name__ == '__main__':
    unittest.main()
