"""Test the execute method of the multiprocess backend when updating a
workflow by inserting, deleting and replacing modules.
"""

import os
import shutil
import time
import unittest

from vizier.engine.packages.pycell.command import python_cell
from vizier.engine.packages.vizual.command import load_dataset
from vizier.api.webservice.base import get_engine
from vizier.config.app import AppConfig

import vizier.config.app as app
import vizier.engine.packages.base as pckg
from vizier.engine.base import compute_context


SERVER_DIR = './.tmp'
PACKAGES_DIR = './tests/engine/workflows/.files/packages'
PROCESSORS_DIR = './tests/engine/workflows/.files/processors'
CSV_FILE = './tests/engine/workflows/.files/people.csv'

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
        os.environ[app.VIZIERENGINE_DATA_DIR] = SERVER_DIR
        os.environ[app.VIZIERSERVER_PACKAGE_PATH] = PACKAGES_DIR
        os.environ[app.VIZIERSERVER_PROCESSOR_PATH] = PROCESSORS_DIR
        os.environ[app.VIZIERENGINE_BACKEND] = 'MULTIPROCESS'
        self.engine = get_engine(AppConfig())

    def tearDown(self):
        """Clean-up by dropping the server directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def assert_module_count_is(self, project, value):
        """Assert number of modules in the workflow."""
        wf = project.viztrail.default_branch.head
        self.assertEqual(len(wf.modules), value)

    def assert_value_is(self, project, value):
        """Assert value of the updated dataset cell."""
        wf = project.viztrail.default_branch.head
        datasets = compute_context(wf.modules)
        if DATASET_NAME not in datasets:
            for m in wf.modules:
                print("WRITES: {}".format(m.provenance.write))
            raise Exception("Expected to see dataset {}, but only had [{}]".format(DATASET_NAME, ", ".join(datasets)))
        ds = project.datastore.get_dataset(datasets[DATASET_NAME].identifier)
        rows = ds.fetch_rows()
        print(rows)
        self.assertEqual(rows[0].values[1], value)

    def create_workflow(self, project):
        """Create a completed workflow by loading the data file and updating the
        age value of the first row ten times.
        """
        branch_id = project.viztrail.default_branch.identifier
        fh = project.filestore.upload_file(CSV_FILE)
        cmd = load_dataset(
            dataset_name=DATASET_NAME,
            file={pckg.FILE_ID: fh.identifier}
        )
        self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=branch_id,
            command=cmd
        )
        for i in range(10):
            cmd = python_cell(PY_ADD_ONE)
            self.engine.append_workflow_module(
                project_id=project.identifier,
                branch_id=branch_id,
                command=cmd
            )
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        for module in project.viztrail.default_branch.head.modules:
            # print("--------=======--------")
            # print(module.command)
            # print(module.outputs)
            # print(module.provenance)
            if not module.is_success:
                print(module.outputs)
            self.assertTrue(module.is_success)
            self.assertTrue(DATASET_NAME in module.provenance.write)
        return branch_id

    def test_delete(self):
        """Test inserting a module."""
        project = self.engine.projects.create_project()
        branch_id = self.create_workflow(project)
        wf = project.viztrail.default_branch.head
        # Keep track of datasets in the completed workflow
        datasets = compute_context(wf.modules)
        datasets = [ datasets[name] for name in datasets if datasets[name].is_dataset]
        # Insert in the middle
        result = self.engine.delete_workflow_module(
            project_id=project.identifier,
            branch_id=branch_id,
            module_id=wf.modules[5].identifier
        )
        self.assertEqual(len(result), 5)
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        wf = project.viztrail.default_branch.head
        self.assert_module_count_is(project, 10)
        self.assert_value_is(project, 32)
        wf = project.viztrail.default_branch.head
        # Ensure that None is returned when attempting to delete a module from
        # an unknown branch
        result = self.engine.delete_workflow_module(
            project_id=project.identifier,
            branch_id='null',
            module_id=wf.modules[-1].identifier
        )
        self.assertIsNone(result)
        # Delete at the end will not result in new datasets being generated
        while len(wf.modules) > 0:
            self.engine.delete_workflow_module(
                project_id=project.identifier,
                branch_id=branch_id,
                module_id=wf.modules[-1].identifier
            )
            wf = project.viztrail.default_branch.head
            self.assertFalse(wf.is_active)

    # def test_insert(self):
    #     """Test inserting a module."""
    #     project = self.engine.projects.create_project()
    #     branch_id = self.create_workflow(project)
    #     wf = project.viztrail.default_branch.head
    #     # Keep track of datasets in the completed workflow
    #     datasets = [m.datasets[DATASET_NAME].identifier for m in wf.modules]
    #     # Insert in the middle
    #     cmd = command=python_cell(PY_ADD_TEN)
    #     result = self.engine.insert_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         before_module_id=wf.modules[5].identifier,
    #         command=cmd
    #     )
    #     self.assertEqual(len(result), 7)
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     self.assert_module_count_is(project, 12)
    #     self.assert_value_is(project, 43)
    #     wf = project.viztrail.default_branch.head
    #     for i in range(5):
    #         self.assertEqual(datasets[i], wf.modules[i].datasets[DATASET_NAME].identifier)
    #     for i in range(5,len(wf.modules)):
    #         self.assertFalse(wf.modules[i].datasets[DATASET_NAME].identifier in datasets)
    #     # Ensure that None is returned when attempting to insert a module into
    #     # an unknown branch
    #     result = self.engine.insert_workflow_module(
    #         project_id=project.identifier,
    #         branch_id='null',
    #         before_module_id=wf.modules[-1].identifier,
    #         command=python_cell('print 2+2')
    #     )
    #     self.assertIsNone(result)
    #     # Insert a neutral python cell at from will have no impact on the other
    #     # modules
    #     self.engine.insert_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         before_module_id=wf.modules[0].identifier,
    #         command=python_cell('print 2+2')
    #     )
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     self.assert_module_count_is(project, 13)
    #     wf = project.viztrail.default_branch.head
    #     for module in wf.modules:
    #         self.assertTrue(module.is_success)
    #     # Inserting an error at the start will leave all moules in error or
    #     # canceled state
    #     self.engine.insert_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         before_module_id=wf.modules[0].identifier,
    #         command=cmd
    #     )
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     self.assert_module_count_is(project, 14)
    #     wf = project.viztrail.default_branch.head
    #     self.assertTrue(wf.modules[0].is_error)
    #     for module in wf.modules[1:]:
    #         self.assertTrue(module.is_canceled)

    # def test_execute(self):
    #     """Test that the initial workflow is created correctly."""
    #     project = self.engine.projects.create_project()
    #     self.create_workflow(project)
    #     self.assertFalse(project.viztrail.default_branch.head.is_active)
    #     self.assert_module_count_is(project, 11)
    #     self.assert_value_is(project, 33)
    #     # Ensure that is returned when attempting to append to an
    #     # unknown branch
    #     result = self.engine.append_workflow_module(
    #         project_id=project.identifier,
    #         branch_id='null',
    #         command=python_cell('print 2+2')
    #     )
    #     self.assertIsNone(result)

    # def test_replace(self):
    #     """Test replacing a module."""
    #     project = self.engine.projects.create_project()
    #     branch_id = self.create_workflow(project)
    #     wf = project.viztrail.default_branch.head
    #     # Keep track of datasets in the completed workflow
    #     datasets = [m.datasets[DATASET_NAME].identifier for m in wf.modules]
    #     # Insert in the middle
    #     cmd = command=python_cell(PY_ADD_TEN)
    #     result = self.engine.replace_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         module_id=wf.modules[5].identifier,
    #         command=cmd
    #     )
    #     self.assertEqual(len(result), 6)
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     self.assert_module_count_is(project, 11)
    #     self.assert_value_is(project, 42)
    #     wf = project.viztrail.default_branch.head
    #     for i in range(5):
    #         self.assertEqual(datasets[i], wf.modules[i].datasets[DATASET_NAME].identifier)
    #     for i in range(5,len(wf.modules)):
    #         self.assertFalse(wf.modules[i].datasets[DATASET_NAME].identifier in datasets)
    #     # Ensure that None is returned when attempting to replace a module in
    #     # an unknown branch
    #     result = self.engine.replace_workflow_module(
    #         project_id=project.identifier,
    #         branch_id='null',
    #         module_id=wf.modules[0].identifier,
    #         command=python_cell('print 2+2')
    #     )
    #     self.assertIsNone(result)
    #     # Replace at the start will leave all moules in error or canceled state
    #     result = self.engine.replace_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         module_id=wf.modules[0].identifier,
    #         command=cmd
    #     )
    #     self.assertEqual(len(result), 11)
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     self.assert_module_count_is(project, 11)
    #     wf = project.viztrail.default_branch.head
    #     self.assertTrue(wf.modules[0].is_error)
    #     for module in wf.modules[1:]:
    #         self.assertTrue(module.is_canceled)

    # def test_skip_modules(self):
    #     """Test replacing a module in a workflow where dome cells do not
    #     require to be re-executed because they access a different dataset.
    #     """
    #     project = self.engine.projects.create_project()
    #     branch_id = project.get_default_branch().identifier
    #     fh1 = project.filestore.upload_file(CSV_FILE)
    #     fh2 = project.filestore.upload_file(CSV_FILE)
    #     self.engine.append_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         command=load_dataset(
    #             dataset_name=DATASET_NAME,
    #             file={pckg.FILE_ID: fh1.identifier}
    #         )
    #     )
    #     self.engine.append_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         command=load_dataset(
    #             dataset_name=SECOND_DATASET_NAME,
    #             file={pckg.FILE_ID: fh2.identifier}
    #         )
    #     )
    #     for i in range(10):
    #         if i in [0,2,4,6,8]:
    #             cmd = command=python_cell(PY_ADD_ONE)
    #         else:
    #             cmd = command=python_cell(PY_ADD_SECOND)
    #         self.engine.append_workflow_module(
    #             project_id=project.identifier,
    #             branch_id=branch_id,
    #             command=cmd
    #         )
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     wf = project.viztrail.default_branch.head
    #     self.assertTrue(wf.get_state().is_success)
    #     datasets = [module.datasets for module in wf.modules[4:]]
    #     self.assert_module_count_is(project, 12)
    #     # Replace a module that updates the first datasets. All modules that
    #     # access the second dataset should remain unchanged.
    #     cmd = command=python_cell(PY_ADD_TEN)
    #     self.engine.replace_workflow_module(
    #         project_id=project.identifier,
    #         branch_id=branch_id,
    #         module_id=wf.modules[4].identifier,
    #         command=cmd
    #     )
    #     while project.viztrail.default_branch.head.is_active:
    #         time.sleep(0.1)
    #     wf = project.viztrail.default_branch.head
    #     self.assertTrue(wf.get_state().is_success)
    #     i = 0
    #     for module in wf.modules[4:]:
    #         self.assertNotEqual(datasets[i][DATASET_NAME].identifier, module.datasets[DATASET_NAME].identifier)
    #         self.assertEqual(datasets[i][SECOND_DATASET_NAME].identifier, module.datasets[SECOND_DATASET_NAME].identifier)
    #         i += 1


if __name__ == '__main__':
    unittest.main()
