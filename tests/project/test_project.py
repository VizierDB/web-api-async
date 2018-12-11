"""Test the execute method of the synchronous backend."""

import os
import shutil
import time
import unittest

from vizier.client.command.pycell import python_cell
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


class TestMultiprocessBackend(unittest.TestCase):

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

    def test_project(self):
        """
        """
        self.assertEquals(self.project.identifier, '000')
        self.assertIsNone(self.project.viztrail.default_branch.head)
        branch_id = self.project.viztrail.default_branch.identifier
        for i in range(5):
            cmd = command=python_cell('print ' + str(i) + ' + ' + str(i))
            self.project.append_workflow_module(branch_id=branch_id, command=cmd)
        time.sleep(10)
        wf = self.project.viztrail.default_branch.head
        self.assertIsNotNone(wf)
        for module in wf.modules:
            print module.state
            for out in module.outputs.stdout:
                print 'STDOUT: ' + out.value
            for out in module.outputs.stderr:
                print 'STDERR: ' + out.value


if __name__ == '__main__':
    unittest.main()
