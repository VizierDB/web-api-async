import os
import unittest

from vizier.datastore.metadata import DatasetMetadata

METADATA_FILE = './data/metadata.yaml'


class TestDatasetMetadata(unittest.TestCase):

    def setUp(self):
        """Delete metadata file if it exists."""
        self.tearDown()


    def tearDown(self):
        """Delete metadata file if it exists."""
        if os.path.isdir(METADATA_FILE):
            os.remove(METADATA_FILE)

    def test_copy_metadata(self):
        """Test adding and retrieving metadata for different object types."""
        ds_meta = DatasetMetadata()
        ds_meta.for_column(1).add('comment', 'Some Comment')
        ds_meta.for_row(0).add('name', 'Nonsense')
        ds_meta.for_cell(1, 1).add('title', 'Nonsense')
        meta = ds_meta.copy_metadata()
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size(), 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size(), 1)
        self.assertEquals(col_anno.find_one('comment').value, 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(col_anno.size(), 1)
        self.assertEquals(row_anno.find_one('name').value, 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size(), 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size(), 1)
        self.assertEquals(cell_anno.find_one('title').value, 'Nonsense')
        # Ensure that changes to the copy don't affect the original
        meta.for_column(1).add('comment', 'New Comment')
        meta.for_column(2).add('comment', 'Some Comment')
        self.assertEquals(meta.for_column(1).size(), 2)
        self.assertEquals(ds_meta.for_column(1).size(), 1)
        self.assertEquals(ds_meta.for_column(1).find_one('comment').value, 'Some Comment')
        values = [a.value for a in meta.for_column(1).values()]
        self.assertTrue('Some Comment' in values)
        self.assertTrue('New Comment' in values)
        self.assertEquals(ds_meta.for_column(2).size(), 0)
        self.assertEquals(meta.for_column(2).size(), 1)
        self.assertEquals(meta.for_column(2).find_one('comment').value, 'Some Comment')

    def test_dataset_metadata(self):
        """Test adding and retrieving metadata for different object types."""
        meta = DatasetMetadata()
        meta.for_column(1).add('comment', 'Some Comment')
        meta.for_row(0).add('name', 'Nonsense')
        meta.for_cell(1, 1).add('title', 'Nonsense')
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size(), 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size(), 1)
        self.assertEquals(col_anno.find_one('comment').value, 'Some Comment')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(col_anno.size(), 1)
        self.assertEquals(row_anno.find_one('name').value, 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size(), 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size(), 1)
        self.assertEquals(cell_anno.find_one('title').value, 'Nonsense')

    def test_io_metadata(self):
        """Test reading and writing metadata from/to file."""
        ds_meta = DatasetMetadata()
        ds_meta.for_column(1).add('comment', 'Some Comment')
        ds_meta.for_column(1).add('name', 'foo')
        ds_meta.for_row(0).add('name', 'Nonsense')
        ds_meta.for_cell(1, 1).add('title', 'Nonsense')
        ds_meta.to_file(METADATA_FILE)
        meta = DatasetMetadata.from_file(METADATA_FILE)
        # Column annotation
        col_anno = meta.for_column(0)
        self.assertEquals(col_anno.size(), 0)
        col_anno = meta.for_column(1)
        self.assertEquals(col_anno.size(), 2)
        self.assertEquals(col_anno.find_one('comment').value, 'Some Comment')
        self.assertEquals(col_anno.find_one('name').value, 'foo')
        # Row annotation
        row_anno = meta.for_row(0)
        self.assertEquals(row_anno.size(), 1)
        self.assertEquals(row_anno.find_one('name').value, 'Nonsense')
        row_anno = meta.for_row(1)
        self.assertEquals(row_anno.size(), 0)
        # Cell Annotations
        cell_anno = meta.for_cell(1, 1)
        self.assertEquals(cell_anno.size(), 1)
        self.assertEquals(cell_anno.find_one('title').value, 'Nonsense')

    def test_update_statements(self):
        """Test update annotation statements."""
        meta = DatasetMetadata()
        # Create a new annotation
        anno1 = meta.for_column(1).update(key='comment', value='Some Comment')
        self.assertEquals(meta.for_column(1).find_one('comment').identifier, anno1.identifier)
        self.assertEquals(meta.for_column(1).find_one('comment').key, 'comment')
        self.assertEquals(meta.for_column(1).find_one('comment').value, 'Some Comment')
        # Create another annotation
        anno2 = meta.for_column(1).update(key='comment', value='Some Comment')
        self.assertEquals(meta.for_column(1).size(), 2)
        # Update first annotation value
        anno1_2 = meta.for_column(1).update(identifier=anno1.identifier, value='Other Comment')
        self.assertEquals(anno1_2.value, 'Other Comment')
        self.assertEquals(meta.for_column(1).get(anno1.identifier).value, 'Other Comment')
        self.assertEquals(meta.for_column(1).get(anno2.identifier).value, 'Some Comment')
        # Delete the second annotation
        meta.for_column(1).update(identifier=anno2.identifier)
        self.assertEquals(meta.for_column(1).size(), 1)
        self.assertEquals(meta.for_column(1).find_one('comment').value, 'Other Comment')
        # Update the key of the remaining annotation
        meta.for_column(1).update(identifier=anno1.identifier, key='remark', value='Other Comment')
        self.assertEquals(meta.for_column(1).size(), 1)
        self.assertIsNone(meta.for_column(1).find_one('commanr'))
        self.assertEquals(meta.for_column(1).find_one('remark').value, 'Other Comment')


if __name__ == '__main__':
    unittest.main()
