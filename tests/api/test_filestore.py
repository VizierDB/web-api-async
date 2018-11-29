import os
import shutil
import unittest

from vizier.api.base import VizierApi
from vizier.client.cli.interpreter import CommandInterpreter
from vizier.config import AppConfig


CONFIG_FILE = './.files/config.yaml'
CSV_FILE = './.files/dataset.csv'
REPO_DIR = './.temp'


class TestFilestoreApi(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)
        config = AppConfig(configuration_file=CONFIG_FILE)
        self.api = VizierApi(
            filestore=config.filestore.create_instance(),
            viztrails_repository=config.viztrails.create_instance()
        )

    def tearDown(self):
        """Delete repository directory."""
        shutil.rmtree(REPO_DIR)

    def test_list_files(self):
        """Test the list files method of the Api."""
        fh = self.api.filestore.upload_file(filename=CSV_FILE)
        files = self.api.filestore.list_files()
        self.assertEquals(len(files), 1)
        self.assertTrue(fh.identifier in [f.identifier for f in files])
        fh = self.api.filestore.upload_file(filename=CSV_FILE)
        files = self.api.filestore.list_files()
        self.assertEquals(len(files), 2)
        self.assertTrue(fh.identifier in [f.identifier for f in files])

    def test_upload_file(self):
        """Test the upload file method of the Api."""
        fh = self.api.filestore.upload_file(filename=CSV_FILE)
        self.assertEquals(fh.name, 'dataset.csv')
        self.assertEquals(fh.file_format, 'csv')


if __name__ == '__main__':
    unittest.main()
