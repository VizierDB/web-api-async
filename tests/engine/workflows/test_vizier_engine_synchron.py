"""Test the default vizier engine with a synchronous backend."""

import os
import shutil
import time
import unittest

from vizier.api.client.command.vizual import load_dataset, update_cell
from vizier.datastore.fs.factory import FileSystemDatastoreFactory
from vizier.engine.base import VizierEngine
from vizier.engine.backend.multiprocess import MultiProcessBackend
from vizier.engine.backend.synchron import SynchronousTaskEngine
from vizier.engine.packages.vizual.api.fs import DefaultVizualApi
from vizier.engine.packages.vizual.processor import VizualTaskProcessor
from vizier.engine.project.cache.common import CommonProjectCache
from vizier.filestore.fs.factory import FileSystemFilestoreFactory
from vizier.viztrail.driver.objectstore.repository import OSViztrailRepository

import vizier.engine.packages.base as pckg
import vizier.engine.packages.vizual.base as vizual


SERVER_DIR = './.tmp'
DATASTORES_DIR = SERVER_DIR + '/ds'
FILESTORES_DIR = SERVER_DIR + '/fs'
VIZTRAILS_DIR = SERVER_DIR + '/vt'

EMPLOYEE_FILE = './.files/employee.csv'
PEOPLE_FILE = './.files/people.csv'


class TestVizierEngineSynchron(unittest.TestCase):

    def setUp(self):
        """Create an instance of the default backend with an empty directory
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)
        os.makedirs(SERVER_DIR)
        self.engine = self.create_engine()

    def tearDown(self):
        """Remove the server directory."""
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def create_engine(self):
        """Create instance of the default vizier engine."""
        projects = CommonProjectCache(
            datastores=FileSystemDatastoreFactory(DATASTORES_DIR),
            filestores=FileSystemFilestoreFactory(FILESTORES_DIR),
            viztrails=OSViztrailRepository(base_path=VIZTRAILS_DIR)
        )
        vizual_processor = VizualTaskProcessor(api=DefaultVizualApi())
        return VizierEngine(
            name='Test Engine',
            version='0.0.1',
            projects=projects,
            backend=MultiProcessBackend(
                processors={vizual.PACKAGE_VIZUAL: vizual_processor},
                projects=projects,
                synchronous=SynchronousTaskEngine(
                    commands={
                        vizual.PACKAGE_VIZUAL: {
                            vizual.VIZUAL_LOAD: vizual_processor,
                            vizual.VIZUAL_UPD_CELL: vizual_processor
                        }
                    },
                    projects=projects
                )
            ),
            packages={
                vizual.PACKAGE_VIZUAL: pckg.PackageIndex(vizual.VIZUAL_COMMANDS)
            }
        )

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
        self.assertTrue('people' in module.datasets)
        self.assertEquals(len(module.datasets['people'].columns), 2)
        self.assertEquals(module.datasets['people'].row_count, 2)
        self.assertEquals(len( module.outputs.stdout), 4)
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
        self.assertTrue(module.is_success)
        self.assertTrue('people' in module.datasets)
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
        self.assertTrue('people' in module.datasets)
        self.assertTrue('employee' in module.datasets)
        #
        # Reload engine and check the module states
        #
        self.engine = self.create_engine()
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        self.assertEquals(len(modules), 3)
        for m in modules:
            self.assertTrue(m.is_success)
            self.assertIsNotNone(m.timestamp.created_at)
            self.assertIsNotNone(m.timestamp.started_at)
            self.assertIsNotNone(m.timestamp.finished_at)
            self.assertIsNotNone(m.provenance.write)
            self.assertTrue('people' in m.datasets)
        self.assertTrue('employee' in modules[-1].datasets)
        self.assertNotEqual(
            modules[0].datasets['people'].identifier,
            modules[1].datasets['people'].identifier
        )
        self.assertEqual(
            modules[1].datasets['people'].identifier,
            modules[2].datasets['people'].identifier
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
        self.assertEquals(len(result), 2)
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        #
        # Reload engine and check the module states
        #
        self.engine = self.create_engine()
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        for m in modules:
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
        self.assertEquals(len(modules), 4)
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
        self.assertEquals(len(result), 5)
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        modules = project.get_default_branch().get_head().modules
        self.assertEquals(len(modules), 5)
        for m in modules:
            self.assertTrue(m.is_success)
        self.assertEquals(len(modules[-1].datasets['friends'].columns), 3)
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
        self.assertEquals(len(result), 5)
        # Wait for the operations to finish
        while project.viztrail.default_branch.head.is_active:
            time.sleep(0.1)
        modules = project.get_default_branch().get_head().modules
        self.assertEquals(len(modules), 5)
        for m in modules:
            self.assertTrue(m.is_success)
        self.assertEquals(len(modules[-1].datasets['friends'].columns), 2)
        ds = project.datastore.get_dataset(modules[-1].datasets['friends'].identifier)
        self.assertEquals(ds.fetch_rows()[0].values[1], 43)
        #
        # Reload engine and check the module states
        #
        self.engine = self.create_engine()
        project = self.engine.projects.get_project(project.identifier)
        modules = project.get_default_branch().get_head().modules
        self.assertEquals(len(modules), 5)
        for m in modules:
            self.assertTrue(m.is_success)
        self.assertEquals(len(modules[-1].datasets['friends'].columns), 2)
        ds = project.datastore.get_dataset(modules[-1].datasets['friends'].identifier)
        self.assertEquals(ds.fetch_rows()[0].values[1], 43)


if __name__ == '__main__':
    unittest.main()
