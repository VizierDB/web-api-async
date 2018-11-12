import os
import shutil
import unittest

from vizier.core.annotation.fs import PersistentAnnotationSet


"""base directory for properties files."""
PROPERTIES_DIRECTORY = './.files/'


class TestPersistentAnnotationSet(unittest.TestCase):

    def setUp(self):
        """Create an empty directory for property files."""
        if os.path.isdir(PROPERTIES_DIRECTORY):
            shutil.rmtree(PROPERTIES_DIRECTORY)
        os.makedirs(PROPERTIES_DIRECTORY)
        self.filename = filename = os.path.join(PROPERTIES_DIRECTORY, 'properties.txt')

    def tearDown(self):
        """Clean-up by deleting the created directory and all the files inside
        it.
        """
        shutil.rmtree(PROPERTIES_DIRECTORY)

    def test_annotation_read_write(self):
        """Test manipulating annotations from an empty set."""
        # Create an empty properties file
        annotations = PersistentAnnotationSet(self.filename)
        self.assertEquals(len(annotations.elements), 0)
        self.assertIsNone(annotations.find_one('A'))
        self.assertFalse(os.path.isfile(self.filename))
        annotations.add('A', 1)
        self.assertTrue(os.path.isfile(self.filename))
        annotations.add('A', 2)
        annotations.add('B', 'XYZ')
        annotations = PersistentAnnotationSet(self.filename)
        self.assertEquals(annotations.find_one('B'), 'XYZ')
        self.assertTrue(1 in annotations.find_all('A'))
        self.assertTrue(2 in annotations.find_all('A'))
        annotations.delete('B', value='XYZ')
        annotations = PersistentAnnotationSet(self.filename)
        self.assertIsNone(annotations.find_one('B'))


if __name__ == '__main__':
    unittest.main()
