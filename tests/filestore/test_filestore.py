"""Test functionality of the default file store."""

import csv
import gzip
import os
import shutil
import sys
import time
import unittest

from werkzeug.datastructures import FileStorage

from vizier.filestore.fs import DefaultFileStore, METADATA_FILE_NAME
import vizier.filestore.base as fs


SERVER_DIR = './.tmp'

CSV_FILE = './.files/dataset.csv'
GZIP_CSV_FILE = './.files/dataset.csv.gz'
TSV_FILE = './.files/dataset.tsv'
GZIP_TSV_FILE = './.files/dataset.tsv.gz'


class TestDefaultFileStore(unittest.TestCase):

    def setUp(self):
        """Create an empty file server repository."""
        # Drop project descriptor directory
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def tearDown(self):
        """Clean-up by dropping file server directory.
        """
        shutil.rmtree(SERVER_DIR)

    def test_cleanup(self):
        """Test clean up function."""
        db = DefaultFileStore(SERVER_DIR)
        fh1 = db.upload_file(CSV_FILE)
        fh2 = db.upload_file(GZIP_CSV_FILE)
        fh3 = db.upload_file(TSV_FILE)
        fh4 = db.upload_file(GZIP_TSV_FILE)
        fh5 = db.upload_file(CSV_FILE)
        fh6 = db.upload_file(GZIP_CSV_FILE)
        fh7 = db.upload_file(TSV_FILE)
        fh8 = db.upload_file(GZIP_TSV_FILE)
        files = db.list_files()
        self.assertEquals(len(files), 8)
        count = db.cleanup(active_files=[fh1.identifier, fh3.identifier, fh7.identifier])
        self.assertEquals(count, 5)
        files = db.list_files()
        self.assertEquals(len(files), 3)
        self.assertIsNotNone(db.get_file(fh1.identifier))
        self.assertIsNotNone(db.get_file(fh3.identifier))
        self.assertIsNotNone(db.get_file(fh7.identifier))
        self.assertIsNone(db.get_file(fh2.identifier))
        self.assertIsNone(db.get_file(fh4.identifier))
        self.assertIsNone(db.get_file(fh5.identifier))
        self.assertIsNone(db.get_file(fh6.identifier))
        self.assertIsNone(db.get_file(fh8.identifier))

    def test_delete_file(self):
        """Test delete file method."""
        db = DefaultFileStore(SERVER_DIR)
        f = db.upload_file(CSV_FILE)
        f = db.get_file(f.identifier)
        self.assertIsNotNone(f)
        self.assertTrue(db.delete_file(f.identifier))
        f = db.get_file(f.identifier)
        self.assertIsNone(f)

    def test_get_file(self):
        """Test file get method."""
        db = DefaultFileStore(SERVER_DIR)
        fh1 = db.upload_file(CSV_FILE)
        fh2 = db.get_file(fh1.identifier)
        self.assertEquals(fh1.identifier, fh2.identifier)
        self.assertEquals(fh1.filepath, fh2.filepath)
        self.assertEquals(fh1.file_format, fh2.file_format)
        # Ensure that the file parses as a CSV file
        with fh1.open() as csvfile:
            rows = 0
            for row in csv.reader(csvfile, delimiter=fh1.delimiter()):
                rows += 1
        self.assertEquals(rows, 3)

    def test_list_file(self):
        """Test list files method."""
        db = DefaultFileStore(SERVER_DIR)
        db.upload_file(CSV_FILE)
        db.upload_file(GZIP_CSV_FILE)
        db.upload_file(TSV_FILE)
        db.upload_file(GZIP_TSV_FILE)
        files = db.list_files()
        self.assertEquals(len(files), 4)
        db.upload_file(CSV_FILE)
        db.upload_file(GZIP_CSV_FILE)
        db.upload_file(TSV_FILE)
        db.upload_file(GZIP_TSV_FILE)
        files = db.list_files()
        self.assertEquals(len(files), 8)

    def test_upload_file(self):
        """Test file upload."""
        db = DefaultFileStore(SERVER_DIR)
        fh = db.upload_file(CSV_FILE)
        self.assertEquals(fh.file_name, os.path.basename(CSV_FILE))
        self.assertEquals(fh.file_format, fs.FORMAT_CSV)
        self.assertEquals(fh.identifier, db.get_file(fh.identifier).identifier)
        self.assertTrue(os.path.isfile(os.path.join(SERVER_DIR, METADATA_FILE_NAME)))
        self.assertTrue(os.path.isfile(fh.filepath))
        # Re-load the repository
        db = DefaultFileStore(SERVER_DIR)
        fh = db.get_file(fh.identifier)
        self.assertEquals(fh.file_name, os.path.basename(CSV_FILE))
        self.assertEquals(fh.file_format, fs.FORMAT_CSV)
        self.assertEquals(fh.identifier, db.get_file(fh.identifier).identifier)
        # Add files with other valid suffixes
        fh = db.upload_file(CSV_FILE)
        self.assertFalse(fh.compressed())
        self.assertEquals(fh.delimiter(), ',')
        fh = db.upload_file(GZIP_CSV_FILE)
        self.assertTrue(fh.compressed())
        self.assertEquals(fh.delimiter(), ',')
        fh = db.upload_file(TSV_FILE)
        self.assertFalse(fh.compressed())
        self.assertEquals(fh.delimiter(), '\t')
        fh = db.upload_file(GZIP_TSV_FILE)
        self.assertTrue(fh.compressed())
        self.assertEquals(fh.delimiter(), '\t')
        # Re-load the repository
        db = DefaultFileStore(SERVER_DIR)
        self.assertEquals(len(db.list_files()), 5)

    def test_upload_stream(self):
        """Test file upload from an open file object."""
        db = DefaultFileStore(SERVER_DIR)
        file = FileStorage(filename=CSV_FILE)
        fh = db.upload_stream(file=file, file_name=os.path.basename(CSV_FILE))
        self.assertEquals(fh.file_name, os.path.basename(CSV_FILE))
        self.assertEquals(fh.file_format, fs.FORMAT_CSV)
        self.assertTrue(os.path.isfile(fh.filepath))
        self.assertEquals(fh.identifier, db.get_file(fh.identifier).identifier)

if __name__ == '__main__':
    unittest.main()
