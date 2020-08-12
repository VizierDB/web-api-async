"""Test functionality of the default file store."""

import csv
import gzip
import os
import shutil
import sys
import time
import unittest

from werkzeug.datastructures import FileStorage

from vizier.filestore.fs.base import FileSystemFilestore, METADATA_FILENAME, PARA_DIRECTORY
import vizier.filestore.base as fs


SERVER_DIR = './.tmp'

CSV_FILE = './tests/filestore/.files/dataset.csv'
GZIP_CSV_FILE = './tests/filestore/.files/dataset.csv.gz'
TSV_FILE = './tests/filestore/.files/dataset.tsv'
GZIP_TSV_FILE = './tests/filestore/.files/dataset.tsv.gz'
TEXT_FILE = './tests/filestore/.files/textfile.txt'


class TestFileSystemFilestore(unittest.TestCase):

    def setUp(self):
        """Create an empty filestore directory."""
        # Delete filestore directory
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def tearDown(self):
        """Clean-up by deleting the filestore directory.
        """
        if os.path.isdir(SERVER_DIR):
            shutil.rmtree(SERVER_DIR)

    def test_delete_file(self):
        """Test delete file method."""
        db = FileSystemFilestore(SERVER_DIR)
        f = db.upload_file(CSV_FILE)
        f = db.get_file(f.identifier)
        self.assertIsNotNone(f)
        self.assertTrue(db.delete_file(f.identifier))
        f = db.get_file(f.identifier)
        self.assertIsNone(f)

    def test_get_file(self):
        """Test file get method."""
        db = FileSystemFilestore(SERVER_DIR)
        fh1 = db.upload_file(CSV_FILE)
        fh2 = db.get_file(fh1.identifier)
        self.assertEqual(fh1.identifier, fh2.identifier)
        self.assertEqual(fh1.filepath, fh2.filepath)
        self.assertEqual(fh1.mimetype, fh2.mimetype)
        # Ensure that the file parses as a CSV file
        with fh1.open() as csvfile:
            rows = 0
            for row in csv.reader(csvfile, delimiter=fh1.delimiter):
                rows += 1
        self.assertEqual(rows, 3)

    def test_list_file(self):
        """Test list files method."""
        db = FileSystemFilestore(SERVER_DIR)
        db.upload_file(CSV_FILE)
        db.upload_file(GZIP_CSV_FILE)
        db.upload_file(TSV_FILE)
        db.upload_file(GZIP_TSV_FILE)
        files = db.list_files()
        self.assertEqual(len(files), 4)
        db.upload_file(CSV_FILE)
        db.upload_file(GZIP_CSV_FILE)
        db.upload_file(TSV_FILE)
        db.upload_file(GZIP_TSV_FILE)
        files = db.list_files()
        self.assertEqual(len(files), 8)

    def test_upload_file(self):
        """Test file upload."""
        db = FileSystemFilestore(SERVER_DIR)
        fh = db.upload_file(CSV_FILE)
        self.assertEqual(fh.file_name, os.path.basename(CSV_FILE))
        self.assertEqual(fh.mimetype, fs.FORMAT_CSV)
        self.assertEqual(fh.identifier, db.get_file(fh.identifier).identifier)
        self.assertTrue(os.path.isfile(os.path.join(SERVER_DIR, fh.identifier, METADATA_FILENAME)))
        self.assertTrue(os.path.isfile(fh.filepath))
        self.assertTrue(fh.is_tabular)
        # Re-load the repository
        db = FileSystemFilestore(SERVER_DIR)
        fh = db.get_file(fh.identifier)
        self.assertEqual(fh.file_name, os.path.basename(CSV_FILE))
        self.assertEqual(fh.mimetype, fs.FORMAT_CSV)
        self.assertEqual(fh.identifier, db.get_file(fh.identifier).identifier)
        # Add files with other valid suffixes
        fh = db.upload_file(CSV_FILE)
        self.assertFalse(fh.compressed)
        self.assertEqual(fh.delimiter, ',')
        fh = db.upload_file(GZIP_CSV_FILE)
        self.assertTrue(fh.compressed)
        self.assertEqual(fh.delimiter, ',')
        fh = db.upload_file(TSV_FILE)
        self.assertFalse(fh.compressed)
        self.assertEqual(fh.delimiter, '\t')
        fh = db.upload_file(GZIP_TSV_FILE)
        self.assertTrue(fh.compressed)
        self.assertEqual(fh.delimiter, '\t')
        # Re-load the repository
        db = FileSystemFilestore(SERVER_DIR)
        self.assertEqual(len(db.list_files()), 5)
        fh = db.upload_file(TEXT_FILE)
        self.assertFalse(fh.is_tabular)

    def test_upload_stream(self):
        """Test file upload from an open file object."""
        db = FileSystemFilestore(SERVER_DIR)
        file = FileStorage(filename=CSV_FILE)
        fh = db.upload_stream(file=file, file_name=os.path.basename(CSV_FILE))
        self.assertEqual(fh.file_name, os.path.basename(CSV_FILE))
        self.assertEqual(fh.mimetype, fs.FORMAT_CSV)
        self.assertTrue(os.path.isfile(fh.filepath))
        self.assertEqual(fh.identifier, db.get_file(fh.identifier).identifier)

if __name__ == '__main__':
    unittest.main()
