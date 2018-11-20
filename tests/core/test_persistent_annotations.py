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

    def test_annotation_init(self):
        """Test initializing the persistent annotation set from a given
        dictionary.
        """
        annotations = PersistentAnnotationSet(
            self.filename,
            annotations={'A': 1, 'B': ['a', 2, 'c']}
        )
        self.assertEquals(annotations.find_one('A'), 1)
        self.assertTrue('a' in annotations.find_all('B'))
        self.assertTrue(2 in annotations.find_all('B'))
        self.assertTrue('c' in annotations.find_all('B'))
        # Reload annotations to ensure they are persistent
        annotations = PersistentAnnotationSet(self.filename)
        self.assertEquals(annotations.find_one('A'), 1)
        self.assertTrue('a' in annotations.find_all('B'))
        self.assertTrue(2 in annotations.find_all('B'))
        self.assertTrue('c' in annotations.find_all('B'))
        # Error when initializing annotation set twice
        with self.assertRaises(ValueError):
            annotations = PersistentAnnotationSet(
                self.filename,
                annotations={'A': 1, 'B': ['a', 2, 'c']}
            )
        # Error when initalizing with invalid object
        os.remove(self.filename)
        with self.assertRaises(ValueError):
            annotations = PersistentAnnotationSet(
                self.filename,
                annotations={'A': 1, 'B': [{'a': 1}, 2, 'c']}
            )

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
