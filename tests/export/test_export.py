"""Test functionality of the default file store factory."""

import os
import unittest
import shutil
from tarfile import open as taropen
import time

from vizier.api.webservice.base import get_engine
from vizier.config.app import AppConfig
import vizier.config.app as app
from vizier import export as viz_export
from vizier.engine.packages.vizual.command import load_dataset
from vizier.engine.packages.pycell.command import python_cell
import vizier.engine.packages.base as pckg

from vizier.config.base import MIMIR_ENGINE
PACKAGES_DIR = './tests/engine/workflows/.files/packages'
PROCESSORS_DIR = './tests/engine/workflows/.files/processors/mimir'

SERVER_DIR = './.tmp'
ARCHIVE_FILE = "export_test.tgz"
CSV_FILE_R = './tests/test_data/r.csv'
DATASET_R = "R"

PY_ADD_ONE = """ds = vizierdb.get_dataset('""" + DATASET_R + """')
a = int(ds.rows[0].get_value('A'))
ds.rows[0].set_value('A', a + 1)
ds.save()
x = 42
vizierdb.export_module(x)
"""

class TestExport(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        os.environ[app.VIZIERSERVER_ENGINE] = MIMIR_ENGINE
        os.environ[app.VIZIERENGINE_DATA_DIR] = SERVER_DIR
        os.environ[app.VIZIERSERVER_PACKAGE_PATH] = PACKAGES_DIR
        os.environ[app.VIZIERSERVER_PROCESSOR_PATH] = PROCESSORS_DIR
        self.engine = get_engine(AppConfig())

        self.project = self.engine.projects.create_project()
        branch_id = self.project.viztrail.default_branch.identifier
        fh = self.project.filestore.upload_file(CSV_FILE_R)
        cmd = load_dataset(
            dataset_name=DATASET_R,
            file={
                pckg.FILE_ID: fh.identifier,
                pckg.FILE_NAME: os.path.basename(CSV_FILE_R)
            },
            infer_types = True
        )
        self.engine.append_workflow_module(
            project_id=self.project.identifier,
            branch_id=branch_id,
            command=cmd
        )
        cmd = python_cell(PY_ADD_ONE)
        self.engine.append_workflow_module(
            project_id=self.project.identifier,
            branch_id=branch_id,
            command=cmd
        )
        wf = self.project.viztrail.default_branch.head
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        for m in wf.modules:
            # print(m)
            assert(m.is_success)

    @classmethod
    def tearDownClass(self):
        if os.path.isfile(ARCHIVE_FILE) and False:
            os.unlink(ARCHIVE_FILE)

    def test_export(self):
        viz_export.export_project(ARCHIVE_FILE, TestExport.project)
        # We'd better have exported the file
        self.assertTrue(os.path.isfile(ARCHIVE_FILE))
        
        # Now inspect it
        with taropen(ARCHIVE_FILE) as archive:
            contents = set(archive.getnames())

            # We expect to have a version file with the correct version
            self.assertIn(viz_export.VERSION_PATH, contents)
            with archive.extractfile(viz_export.VERSION_PATH) as f:
                self.assertEqual(f.read().decode(), viz_export.EXPORT_VERSION)

    def test_import(self):
        viz_export.import_project(ARCHIVE_FILE, TestExport.engine)


if __name__ == '__main__':
    unittest.main()
