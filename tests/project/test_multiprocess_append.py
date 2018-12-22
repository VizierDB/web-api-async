"""Test the execute method of the multiprocess backend when appending modules
only to an existing workflow.
"""

import os
import shutil
import time
import unittest

from vizier.api.client.command.pycell import python_cell
from vizier.api.client.command.vizual import load_dataset
from vizier.datastore.fs.base import FileSystemDatastore
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.packages.pycell.processor import PyCellTaskProcessor
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.processor import VizualTaskProcessor
from vizier.engine.project import ProjectHandle
from vizier.filestore.fs.base import DefaultFilestore
from vizier.viztrail.driver.objectstore.viztrail import OSViztrailHandle

import vizier.engine.packages.base as pckg
import vizier.engine.packages.pycell.base as pycell
import vizier.engine.packages.vizual.base as vizual


SERVER_DIR = './.tmp'
FILESTORE_DIR = './.tmp/fs'
DATASTORE_DIR = './.tmp/ds'
REPO_DIR = './.temp/vt'

CSV_FILE = './.files/dataset.csv'

DATASET_NAME = 'abc'

PY_ADD_ONE_ERROR = """ds = vizierdb.get_dataset('""" + DATASET_NAME + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 1)
vizierdb.update_dataset('""" + DATASET_NAME + """, ds')
"""


class TestMultiprocessBackendAppend(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default vizier processor for an empty server
        directory.
        """
        # Drop directory if it exists
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        datastore = FileSystemDatastore(DATASTORE_DIR)
        filestore = DefaultFilestore(FILESTORE_DIR)
        backend = MultiProcessBackend(
            processors={
                pycell.PACKAGE_PYTHON: PyCellTaskProcessor(),
                vizual.PACKAGE_VIZUAL:  VizualTaskProcessor(api=DefaultVizualApi())
            }
        )
        base_path = os.path.join(os.path.join(SERVER_DIR, REPO_DIR, '000'))
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='000',
            properties=None,
            base_path=base_path
        )
        self.project = ProjectHandle(
            viztrail=vt,
            datastore=datastore,
            filestore=filestore,
            backend=backend,
            packages={
                pycell.PACKAGE_PYTHON: pckg.PackageIndex(pycell.PYTHON_COMMANDS),
                vizual.PACKAGE_VIZUAL: pckg.PackageIndex(vizual.VIZUAL_COMMANDS)
            }
        )

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_cancel(self):
        """Test cancelling a running workflow."""
        branch_id = self.project.viztrail.default_branch.identifier
        for i in range(10):
            cmd = command=python_cell('import time\ntime.sleep(' + str(i) + ')\nprint \'DONE\'')
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        time.sleep(3)
        self.project.cancel_exec(branch_id)
        wf = self.project.viztrail.default_branch.head
        self.assertIsNotNone(wf)
        canceled_count = 0
        for i in range(10):
            module = wf.modules[i]
            self.assertTrue(module.is_success or module.is_canceled)
            if module.is_success:
                self.assertEquals(len(module.outputs.stdout), 1)
                self.assertEquals(module.outputs.stdout[0].value, 'DONE')
            else:
                canceled_count += 1
        self.assertTrue(canceled_count > 0)
        # All workflows in the branch history should be finished as well
        self.assertEquals(len(self.project.viztrail.default_branch.get_history()), 10)
        for wf in self.project.viztrail.default_branch.get_history():
            self.assertFalse(wf.is_active)

    def test_error(self):
        """Test running a sequence of steps where one generates an error."""
        branch_id = self.project.viztrail.default_branch.identifier
        for i in range(10):
            if i != 5:
                cmd = command=python_cell('import time\ntime.sleep(' + str(i) + ')\nprint \'DONE\'')
            else:
                cmd = command=python_cell('a += 1')
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = self.project.viztrail.default_branch.head
        self.assertIsNotNone(wf)
        for i in range(5):
            module = wf.modules[i]
            if i < 5:
                self.assertTrue(module.is_success)
            elif i == 5:
                self.assertTrue(module.is_error)
            else:
                self.assertTrue(module.is_canceled)
        # All workflows in the branch history should be finished as well
        self.assertEquals(len(self.project.viztrail.default_branch.get_history()), 10)
        for wf in self.project.viztrail.default_branch.get_history():
            self.assertFalse(wf.is_active)

    def test_execute_with_error(self):
        """Test running a sequence of statements where we (potentially)append to
        a workflow that in in error state.
        """
        branch_id = self.project.viztrail.default_branch.identifier
        fh = self.project.filestore.upload_file(CSV_FILE)
        cmd = load_dataset(
            dataset_name=DATASET_NAME,
            file={pckg.FILE_ID: fh.identifier}
        )
        self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        for i in range(20):
            cmd = command=python_cell(PY_ADD_ONE_ERROR)
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = self.project.viztrail.default_branch.head
        self.assertIsNotNone(wf)
        # The second module will raise an error. All following modules should
        # be canceled
        for i in range(20):
            module = wf.modules[i]
            if i == 0:
                self.assertTrue(module.is_success)
            elif i == 1:
                self.assertTrue(module.is_error)
            else:
                self.assertTrue(module.is_canceled)

    def test_run_modules(self):
        """Test running a sequence of statements and wait for their
        completion.
        """
        self.assertEquals(self.project.identifier, '000')
        self.assertIsNone(self.project.viztrail.default_branch.head)
        branch_id = self.project.viztrail.default_branch.identifier
        for i in range(10):
            cmd = command=python_cell('print ' + str(i) + ' + ' + str(i))
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = self.project.viztrail.default_branch.head
        self.assertIsNotNone(wf)
        for i in range(10):
            module = wf.modules[i]
            self.assertTrue(module.is_success)
            self.assertEquals(len(module.outputs.stdout), 1)
            self.assertEquals(module.outputs.stdout[0].value, str(i+i))
            self.assertEquals(len(module.outputs.stderr), 0)
        # All workflows in the branch history should be finished as well
        self.assertEquals(len(self.project.viztrail.default_branch.get_history()), 10)
        for wf in self.project.viztrail.default_branch.get_history():
            self.assertFalse(wf.is_active)


if __name__ == '__main__':
    unittest.main()
