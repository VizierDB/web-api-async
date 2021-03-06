"""Test the default vizier engine with a synchronous backend."""

import os
import shutil
import time
import unittest

from vizier.engine.packages.vizual.command import load_dataset, update_cell
from vizier.api.webservice.base import get_engine
from vizier.config.app import AppConfig

import vizier.config.app as app
import vizier.engine.packages.base as pckg
import vizier.engine.packages.vizual.base as vizual


SERVER_DIR = './.tmp'
PACKAGES_DIR = './tests/engine/workflows/.files/packages'
PROCESSORS_DIR = './tests/engine/workflows/.files/processors/mimir'

EMPLOYEE_FILE = './tests/engine/workflows/.files/employee.csv'
PEOPLE_FILE = './tests/engine/workflows/.files/people.csv'


class TestVizierEngineSynchron(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default backend with an empty directory
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        os.environ[app.VIZIERENGINE_DATA_DIR] = SERVER_DIR
        os.environ[app.VIZIERSERVER_PACKAGE_PATH] = PACKAGES_DIR
        os.environ[app.VIZIERSERVER_PROCESSOR_PATH] = PROCESSORS_DIR
        os.environ[app.VIZIERENGINE_SYNCHRONOUS] = ':'.join([
            vizual.PACKAGE_DATA   + '.' + vizual.VIZUAL_LOAD,
            vizual.PACKAGE_VIZUAL + '.' + vizual.VIZUAL_LOAD,
            vizual.PACKAGE_VIZUAL + '.' + vizual.VIZUAL_UPD_CELL
        ])
        self.engine = get_engine(AppConfig())

    def tearDown(self):
        """Remove the server directory."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_create_synchronous_workflow(self):
        """Create workflow by appending a sequence of modules that are executed
        synchronously.
        """
        project = self.engine.projects.create_project()
        # MODULE 1
        # --------
        # LOAD people
        fh = project.filestore.upload_file(PEOPLE_FILE)
        module = self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            command=load_dataset(
                dataset_name='people',
                file={pckg.FILE_ID: fh.identifier},
                validate=True
            )
        )
        self.assertTrue(module.is_success)
        self.assertTrue('people' in module.provenance.write)
        self.assertEqual(len(module.provenance.write['people'].columns), 2)
        # MODULE 2
        # --------
        # UPDATE CELL
        module = self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            command=update_cell(
                dataset_name='people',
                column=1,
                row=0,
                value='42',
                validate=True
            )
        )
        print("STATUS: {}".format(module))
        self.assertTrue(module.is_success)
        self.assertTrue('people' in module.provenance.write)
        # MODULE 3
        # --------
        # LOAD employee
        fh = project.filestore.upload_file(EMPLOYEE_FILE)
        module = self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            command=load_dataset(
                dataset_name='employee',
                file={pckg.FILE_ID: fh.identifier},
                validate=True
            )
        )
        self.assertTrue(module.is_success)
        self.assertFalse('people' in module.provenance.write)
        self.assertTrue('employee' in module.provenance.write)
        #
        # Reload engine and check the module states
        #
        self.engine = get_engine(AppConfig())
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        self.assertEqual(len(modules), 3)
        for m in modules:
            self.assertTrue(m.is_success)
            self.assertIsNotNone(m.timestamp.created_at)
            self.assertIsNotNone(m.timestamp.started_at)
            self.assertIsNotNone(m.timestamp.finished_at)
            self.assertIsNotNone(m.provenance.write)
        self.assertTrue('people' in modules[0].provenance.write)
        self.assertTrue('employee' in modules[-1].provenance.write)
        self.assertNotEqual(
            modules[0].provenance.write['people'].identifier,
            modules[1].provenance.write['people'].identifier
        )

    def test_create_synchronous_workflow_with_errors(self):
        """Create workflow by appending a sequence of modules that are executed
        synchronously.
        """
        project = self.engine.projects.create_project()
        # MODULE 1
        # --------
        # LOAD people
        fh = project.filestore.upload_file(PEOPLE_FILE)
        module = self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            command=load_dataset(
                dataset_name='people',
                file={
                    pckg.FILE_ID: fh.identifier,
                    pckg.FILE_NAME: os.path.basename(PEOPLE_FILE)
                },
                validate=True
            )
        )
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        for m in modules:
            print(m)
            self.assertTrue(m.is_success)
        # MODULE 2
        # --------
        # UPDATE CELL
        module = self.engine.append_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            command=update_cell(
                dataset_name='employee',
                column=1,
                row=0,
                value='42',
                validate=True
            )
        )
        self.assertTrue(module.is_error)
        # MODULE 2
        # --------
        # INSERT employee
        fh = project.filestore.upload_file(EMPLOYEE_FILE)
        result = self.engine.insert_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            before_module_id=module.identifier,
            command=load_dataset(
                dataset_name='employee',
                file={
                    pckg.FILE_ID: fh.identifier,
                    pckg.FILE_NAME: os.path.basename(EMPLOYEE_FILE)
                },
                validate=True
            )
        )
        self.assertEqual(len(result), 2)
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        #
        # Reload engine and check the module states
        #
        self.engine = get_engine(AppConfig())
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        for m in modules:
            print(m)
            self.assertTrue(m.is_success)
        # MODULE 1
        # --------
        # UPDATE CELL
        module = self.engine.insert_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            before_module_id=modules[0].identifier,
            command=update_cell(
                dataset_name='friends',
                column=1,
                row=0,
                value='43',
                validate=True
            )
        )
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        modules = project.get_default_branch().get_head().modules
        self.assertEqual(len(modules), 4)
        self.assertTrue(modules[0].is_error)
        for m in modules[1:]:
            self.assertTrue(m.is_canceled)
        # MODULE 1
        # --------
        # INSERT friends
        fh = project.filestore.upload_file(EMPLOYEE_FILE)
        result = self.engine.insert_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            before_module_id=modules[0].identifier,
            command=load_dataset(
                dataset_name='friends',
                file={
                    pckg.FILE_ID: fh.identifier,
                    pckg.FILE_NAME: os.path.basename(EMPLOYEE_FILE)
                },
                validate=True
            )
        )
        self.assertEqual(len(result), 5)
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        modules = project.get_default_branch().get_head().modules
        self.assertEqual(len(modules), 5)
        for m in modules:
            self.assertTrue(m.is_success)
        self.assertEqual(len(modules[0].provenance.write['friends'].columns), 3)
        # REPLACE MODULE 1
        # ----------------
        # Load people dataset instead employee
        fh = project.filestore.upload_file(PEOPLE_FILE)
        result = self.engine.replace_workflow_module(
            project_id=project.identifier,
            branch_id=project.get_default_branch().identifier,
            module_id=modules[0].identifier,
            command=load_dataset(
                dataset_name='friends',
                file={
                    pckg.FILE_ID: fh.identifier,
                    pckg.FILE_NAME: os.path.basename(PEOPLE_FILE)
                },
                validate=True
            )
        )
        self.assertEqual(len(result), 5)
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        modules = project.get_default_branch().get_head().modules
        self.assertEqual(len(modules), 5)
        for m in modules:
            self.assertTrue(m.is_success)
        self.assertEqual(len(modules[0].provenance.write['friends'].columns), 2)
        ds = project.datastore.get_dataset(modules[0].provenance.write['friends'].identifier)
        self.assertEqual(ds.fetch_rows()[0].values[1], 23)
        #
        # Reload engine and check the module states
        #
        self.engine = get_engine(AppConfig())
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        self.assertEqual(len(modules), 5)
        for m in modules:
            self.assertTrue(m.is_success)
        self.assertEqual(len(modules[0].provenance.write['friends'].columns), 2)
        ds = project.datastore.get_dataset(modules[0].provenance.write['friends'].identifier)
        self.assertEqual(ds.fetch_rows()[0].values[1], 23)


if __name__ == '__main__':
    unittest.main()
