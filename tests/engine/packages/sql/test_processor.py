"""Test SQL processor."""

import os
import shutil
import unittest

from vizier.datastore.mimir.store import MimirDatastore
from vizier.engine.packages.sql.command import sql_cell
from vizier.engine.packages.sql.processor import SQLTaskProcessor
from vizier.engine.task.base import TaskContext
from vizier.filestore.fs.base import FileSystemFilestore

import vizier.mimir as mimir

SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'

CSV_FILE = './tests/engine/packages/sql/.files/w49k-mmkh.csv'
DATASET_NAME = 'stats'

class TestSQLProcessor(unittest.TestCase):

    def setUp(self):
        """Create an instance of the Mimir processor for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.datastore = MimirDatastore(DATASTORE_DIR)
        self.filestore = FileSystemFilestore(FILESTORE_DIR)

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_run_sql_query(self):
        """Test running a SQL query without materializing the result."""
        f_handle = self.filestore.upload_file(CSV_FILE)
        ds = self.datastore.load_dataset(f_handle=f_handle)
        cmd = sql_cell(
            source='SELECT grade_or_service_category_ FROM ' + DATASET_NAME + ' WHERE program = \'GENERAL EDUCATION\'',
            validate=True
        )
        result = SQLTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                project_id=3,
                artifacts={DATASET_NAME: ds},
                datastore=self.datastore,
                filestore=self.filestore
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        # Materialize result
        cmd = sql_cell(
            source='SELECT grade_or_service_category_ FROM ' + DATASET_NAME + ' WHERE program = \'GENERAL EDUCATION\'',
            output_dataset='ge',
            validate=True
        )
        result = SQLTaskProcessor().compute(
            command_id=cmd.command_id,
            arguments=cmd.arguments,
            context=TaskContext(
                project_id=3,
                artifacts={DATASET_NAME: ds},
                datastore=self.datastore,
                filestore=self.filestore
            )
        )
        self.assertTrue(result.is_success)
        self.assertIsNotNone(result.provenance.read)
        self.assertIsNotNone(result.provenance.write)
        self.assertTrue('ge' in result.provenance.write)


if __name__ == '__main__':
    unittest.main()
