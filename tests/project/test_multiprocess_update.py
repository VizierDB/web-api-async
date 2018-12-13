"""Test the execute method of the multiprocess backend when updating a
workflow by inserting, deleting and replacing modules.
"""

import os
import shutil
import time
import unittest

from vizier.client.command.pycell import python_cell
from vizier.client.command.vizual import load_dataset
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

DATASET_NAME = 'people'
SECOND_DATASET_NAME = 'employee'

PY_ADD_ONE = """ds = vizierdb.get_dataset('""" + DATASET_NAME + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 1)
vizierdb.update_dataset('""" + DATASET_NAME + """', ds)
"""

PY_ADD_SECOND = """ds = vizierdb.get_dataset('""" + SECOND_DATASET_NAME + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 1)
vizierdb.update_dataset('""" + SECOND_DATASET_NAME + """', ds)
"""

PY_ADD_ONE_ERROR = """ds = vizierdb.get_dataset('""" + DATASET_NAME + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 1)
vizierdb.update_dataset('""" + DATASET_NAME + """, ds')
"""

PY_ADD_TEN = """ds = vizierdb.get_dataset('""" + DATASET_NAME + """')
age = int(ds.rows[0].get_value('Age'))
ds.rows[0].set_value('Age', age + 10)
vizierdb.update_dataset('""" + DATASET_NAME + """', ds)
"""


class TestMultiprocessBackendUpdate(unittest.TestCase):

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
            datastore=datastore,
            filestore=filestore,
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

    def assert_module_count_is(self, value):
        """Assert number of modules in the workflow."""
        wf = self.project.viztrail.default_branch.head
        self.assertEquals(len(wf.modules), value)

    def assert_value_is(self, value):
        """Assert value of the updated dataset cell."""
        wf = self.project.viztrail.default_branch.head
        ds = self.project.datastore.get_dataset(wf.modules[-1].datasets[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        self.assertEquals(rows[0].values[1], value)

    def create_workflow(self):
        """Create a completed workflow by loading the data file and updating the
        age value of the first row ten times.
        """
        branch_id = self.project.viztrail.default_branch.identifier
        fh = self.project.filestore.upload_file(CSV_FILE)
        cmd = load_dataset(
            dataset_name=DATASET_NAME,
            file_id={pckg.FILE_ID: fh.identifier}
        )
        self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        for i in range(10):
            cmd = command=python_cell(PY_ADD_ONE)
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        return branch_id

    def test_delete(self):
        """Test inserting a module."""
        branch_id = self.create_workflow()
        wf = self.project.viztrail.default_branch.head
        # Keep track of datasets in the completed workflow
        datasets = [m.datasets[DATASET_NAME].identifier for m in wf.modules]
        # Insert in the middle
        result = self.project.delete_workflow_module(
            branch_id=branch_id,
            module_id=wf.modules[5].identifier
        )
        self.assertEquals(len(result), 5)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = self.project.viztrail.default_branch.head
        self.assert_module_count_is(10)
        self.assert_value_is(32)
        wf = self.project.viztrail.default_branch.head
        for i in range(5):
            self.assertEquals(datasets[i], wf.modules[i].datasets[DATASET_NAME].identifier)
        for i in range(5,len(wf.modules)):
            self.assertFalse(wf.modules[i].datasets[DATASET_NAME].identifier in datasets)
        datasets = [m.datasets[DATASET_NAME].identifier for m in wf.modules]
        # Ensure that an error is raised when attempting to delete a module from
        # an unknown branch
        with self.assertRaises(ValueError):
            self.project.delete_workflow_module(
                branch_id='null',
                module_id=wf.modules[-1].identifier
            )
        # Delete at the end will not result in new datasets being generated
        while len(wf.modules) > 0:
            self.project.delete_workflow_module(branch_id=branch_id, module_id=wf.modules[-1].identifier)
            wf = self.project.viztrail.default_branch.head
            self.assertFalse(wf.is_active)
            for module in wf.modules:
                self.assertTrue(module.datasets[DATASET_NAME].identifier in datasets)

    def test_insert(self):
        """Test inserting a module."""
        branch_id = self.create_workflow()
        wf = self.project.viztrail.default_branch.head
        # Keep track of datasets in the completed workflow
        datasets = [m.datasets[DATASET_NAME].identifier for m in wf.modules]
        # Insert in the middle
        cmd = command=python_cell(PY_ADD_TEN)
        result = self.project.insert_workflow_module(
            branch_id=branch_id,
            before_module_id=wf.modules[5].identifier,
            command=cmd
        )
        self.assertEquals(len(result), 7)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_module_count_is(12)
        self.assert_value_is(43)
        wf = self.project.viztrail.default_branch.head
        for i in range(5):
            self.assertEquals(datasets[i], wf.modules[i].datasets[DATASET_NAME].identifier)
        for i in range(5,len(wf.modules)):
            self.assertFalse(wf.modules[i].datasets[DATASET_NAME].identifier in datasets)
        # Ensure that an error is raised when attempting to insert a module into
        # an unknown branch
        with self.assertRaises(ValueError):
            self.project.insert_workflow_module(
                branch_id='null',
                before_module_id=wf.modules[-1].identifier,
                command=python_cell('print 2+2')
            )
        # Insert a neutral python cell at from will have no impact on the other
        # modules
        self.project.insert_workflow_module(
            branch_id=branch_id,
            before_module_id=wf.modules[0].identifier,
            command=python_cell('print 2+2')
        )
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_module_count_is(13)
        wf = self.project.viztrail.default_branch.head
        for module in wf.modules:
            self.assertTrue(module.is_success)
        # Inserting an error at the start will leave all moules in error or
        # canceled state
        self.project.insert_workflow_module(
            branch_id=branch_id,
            before_module_id=wf.modules[0].identifier,
            command=cmd
        )
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_module_count_is(14)
        wf = self.project.viztrail.default_branch.head
        self.assertTrue(wf.modules[0].is_error)
        for module in wf.modules[1:]:
            self.assertTrue(module.is_canceled)

    def test_execute(self):
        """Test that the initial workflow is created correctly."""
        self.create_workflow()
        self.assertFalse(self.project.viztrail.default_branch.head.is_active)
        self.assert_module_count_is(11)
        self.assert_value_is(33)
        # Ensure that an error is raised when attempting to append to an
        # unknown branch
        with self.assertRaises(ValueError):
            self.project.append_workflow_module(
                branch_id='null',
                command=python_cell('print 2+2')
            )

    def test_replace(self):
        """Test replacing a module."""
        branch_id = self.create_workflow()
        wf = self.project.viztrail.default_branch.head
        # Keep track of datasets in the completed workflow
        datasets = [m.datasets[DATASET_NAME].identifier for m in wf.modules]
        # Insert in the middle
        cmd = command=python_cell(PY_ADD_TEN)
        result = self.project.replace_workflow_module(
            branch_id=branch_id,
            module_id=wf.modules[5].identifier,
            command=cmd
        )
        self.assertEquals(len(result), 6)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_module_count_is(11)
        self.assert_value_is(42)
        wf = self.project.viztrail.default_branch.head
        for i in range(5):
            self.assertEquals(datasets[i], wf.modules[i].datasets[DATASET_NAME].identifier)
        for i in range(5,len(wf.modules)):
            self.assertFalse(wf.modules[i].datasets[DATASET_NAME].identifier in datasets)
        # Ensure that an error is raised when attempting to replace a module in
        # an unknown branch
        with self.assertRaises(ValueError):
            self.project.replace_workflow_module(
                branch_id='null',
                module_id=wf.modules[0].identifier,
                command=python_cell('print 2+2')
            )
        # Replace at the start will leave all moules in error or canceled state
        result = self.project.replace_workflow_module(
            branch_id=branch_id,
            module_id=wf.modules[0].identifier,
            command=cmd
        )
        self.assertEquals(len(result), 11)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        self.assert_module_count_is(11)
        wf = self.project.viztrail.default_branch.head
        self.assertTrue(wf.modules[0].is_error)
        for module in wf.modules[1:]:
            self.assertTrue(module.is_canceled)

    def test_skip_modules(self):
        """Test replacing a module in a workflow where dome cells do not
        require to be re-executed because they access a different dataset.
        """
        branch_id = self.project.viztrail.default_branch.identifier
        fh1 = self.project.filestore.upload_file(CSV_FILE)
        fh2 = self.project.filestore.upload_file(CSV_FILE)
        cmd = load_dataset(
            dataset_name=DATASET_NAME,
            file_id={pckg.FILE_ID: fh1.identifier}
        )
        self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        cmd = load_dataset(
            dataset_name=SECOND_DATASET_NAME,
            file_id={pckg.FILE_ID: fh2.identifier}
        )
        self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        for i in range(10):
            if i in [0,2,4,6,8]:
                cmd = command=python_cell(PY_ADD_ONE)
            else:
                cmd = command=python_cell(PY_ADD_SECOND)
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = self.project.viztrail.default_branch.head
        self.assertTrue(wf.get_state().is_success)
        datasets = [module.datasets for module in wf.modules[4:]]
        self.assert_module_count_is(12)
        # Replace a module that updates the first datasets. All modules that
        # access the second dataset should remain unchanged.
        cmd = command=python_cell(PY_ADD_TEN)
        self.project.replace_workflow_module(
            branch_id=branch_id,
            module_id=wf.modules[4].identifier,
            command=cmd
        )
        while self.project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = self.project.viztrail.default_branch.head
        self.assertTrue(wf.get_state().is_success)
        i = 0
        for module in wf.modules[4:]:
            self.assertNotEqual(datasets[i][DATASET_NAME].identifier, module.datasets[DATASET_NAME].identifier)
            self.assertEquals(datasets[i][SECOND_DATASET_NAME].identifier, module.datasets[SECOND_DATASET_NAME].identifier)
            i += 1


if __name__ == '__main__':
    unittest.main()
