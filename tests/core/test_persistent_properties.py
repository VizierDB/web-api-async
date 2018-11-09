import os
import shutil
import unittest

from vizier.core.properties.fs import PersistentObjectProperties


"""base directory for properties files."""
PROPERTIES_DIRECTORY = './.files/'


class TestPersistentObjectProperties(unittest.TestCase):

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

    def test_create_properties(self):
        """Test functionality to create a new properties file."""
        # Create an empty properties file
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(len(props.properties), 0)
        self.assertIsNone(props.get_property('A'))
        with self.assertRaises(ValueError):
            props = PersistentObjectProperties(self.filename, {'A': 1})
        os.remove(self.filename)
        props = PersistentObjectProperties(self.filename, {'A': 1, 'B': 'XYZ'})
        self.assertEquals(len(props.properties), 2)
        self.assertEquals(props.get_property('A'), 1)
        self.assertEquals(props.get_property('B'), 'XYZ')
        # Using default values has no persistent effect
        self.assertIsNone(props.get_property('C'))
        self.assertEquals(props.get_property('C', default_value='X'), 'X')
        self.assertIsNone(props.get_property('C'))
        # Re-load
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(props.get_property('A'), 1)
        self.assertEquals(props.get_property('B'), 'XYZ')
        self.assertIsNone(props.get_property('C'))

    def test_delete_properties(self):
        """Test the deletion of a properties file."""
        props = PersistentObjectProperties(self.filename, {'A': 1})
        # Re-load properties
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(props.get_property('A'), 1)
        # Delete same property twice
        self.assertTrue(props.delete_property('A'))
        self.assertFalse(props.delete_property('A'))
        # Re-load properties
        props = PersistentObjectProperties(self.filename)
        self.assertIsNone(props.get_property('A'))

    def test_update_properties(self):
        """Test handeling of default properties."""
        props = PersistentObjectProperties(self.filename, {'A': 1})
        self.assertEquals(props.get_property('A'), 1)
        # Re-load from disk
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(props.get_property('A'), 1)
        # Update existing property
        props.set_property('A', 2)
        self.assertEquals(props.get_property('A'), 2)
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(props.get_property('A'), 2)
        props.set_property('A', 'XYZ')
        self.assertEquals(props.get_property('A'), 'XYZ')
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(props.get_property('A'), 'XYZ')
        # Update new property
        props.set_property('B', 100)
        self.assertEquals(props.get_property('B'), 100)
        props = PersistentObjectProperties(self.filename)
        self.assertEquals(props.get_property('B'), 100)


if __name__ == '__main__':
    unittest.main()
