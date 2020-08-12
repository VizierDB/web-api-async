import os
import tempfile
import unittest

from vizier.datastore.dataset import DatasetRow
from vizier.datastore.reader import DelimitedFileReader, DefaultJsonDatasetReader


CSV_FILE = './tests/datastore/.files/dataset.csv'
GZIP_CSV_FILE = './tests/datastore/.files/dataset.csv.gz'
TSV_FILE = './tests/datastore/.files/dataset.tsv'
GZIP_TSV_FILE = './tests/datastore/.files/dataset.tsv.gz'
JSON_FILE = './tests/datastore/.files/dataset.json'


class TestDatasetReader(unittest.TestCase):

    def test_delimited_file_reader(self):
        """Test functionality of the delimited file dataset reader."""
        reader = DelimitedFileReader(CSV_FILE)
        # There should be an exception when calling next on a reader that is
        # not open
        with self.assertRaises(StopIteration):
            next(reader)
        reader = reader.open()
        headline = next(reader)
        self.assertEqual(headline.values, ['Name','Age','Salary'])
        next(reader)
        next(reader)
        reader.close()
        # There should be an exception when calling next on a reader that has
        # been closed
        with self.assertRaises(StopIteration):
            next(reader)
        # Ensure that the different file formats are parsed correctly
        self.read_dataset(DelimitedFileReader(CSV_FILE))
        self.read_dataset(DelimitedFileReader(GZIP_CSV_FILE, compressed=True))
        self.read_dataset(DelimitedFileReader(TSV_FILE, delimiter='\t'))
        self.read_dataset(DelimitedFileReader(GZIP_TSV_FILE, delimiter='\t', compressed=True))

    def test_default_json_reader(self):
        """Test functionality of Json dataset reader."""
        reader = DefaultJsonDatasetReader(JSON_FILE)
        with self.assertRaises(StopIteration):
            next(reader)
        count = 0
        with reader.open() as r:
            for row in r:
                self.assertEqual(len(row.values), 3)
                self.assertEqual(row.identifier, count)
                count += 1
        self.assertEqual(count, 2)
        with self.assertRaises(StopIteration):
            next(reader)
        # Create a new dataset and read it
        tmp_file = tempfile.mkstemp()[1]
        reader = DefaultJsonDatasetReader(tmp_file)
        values = ['A', 'B', 1, 2]
        rows = [
            DatasetRow(0, values),
            DatasetRow(1, values),
            DatasetRow(2, values)
        ]
        reader.write(rows)
        count = 0
        with reader.open() as reader:
            for row in reader:
                self.assertEqual(len(row.values), 4)
                self.assertEqual(row.identifier, count)
                count += 1
        self.assertEqual(count, len(rows))
        os.remove(tmp_file)

    def read_dataset(self, reader):
        """The reader should contain three rows with three values each."""
        count = 0
        with reader.open() as r:
            for row in r:
                self.assertEqual(len(row.values), 3)
                self.assertEqual(row.identifier, count)
                count += 1
        self.assertEqual(count, 3)
        with self.assertRaises(StopIteration):
            next(reader)


if __name__ == '__main__':
    unittest.main()
