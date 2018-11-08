import os
import shutil
import sys
import time
import unittest

from vizier.core.properties import FilePropertiesHandler


"""MongoDB database and collection used for test purposes."""
PROPERTIES_DIRECTORY = './env/.properties'


class TestFilePropertiesHandler(unittest.TestCase):

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

        # Creating new properties file without default properties raises an
        # exception
        with self.assertRaises(ValueError):
            FilePropertiesHandler(self.filename)
        props = FilePropertiesHandler(self.filename, {'A': 1})
        self.assertEquals(props.get_properties()['A'], 1)

    def test_default_properties(self):
        """Test handeling of default properties."""
        FilePropertiesHandler(self.filename, {'A': 1})
        props = FilePropertiesHandler(self.filename, {'A': 2, 'B': 3})
        self.assertEquals(props.get_properties()['A'], 1)
        self.assertEquals(props.get_properties()['B'], 3)

    def test_delete_properties(self):
        """Test the deletion of a properties file."""
        props = FilePropertiesHandler(self.filename, {'A': 1})
        # Ensure that the properties file exists
        self.assertTrue(os.path.isfile(self.filename))
        props.delete_properties()
        self.assertFalse(os.path.isfile(self.filename))

    def test_update_properties(self):
        """Test handeling of default properties."""
        props = FilePropertiesHandler(self.filename, {'A': 1, 'B': 2})
        self.assertEquals(props.get_properties()['A'], 1)
        props.update_properties({'A': None, 'B': 3, 'C': 4})
        self.assertFalse('A' in props.get_properties())
        self.assertEquals(props.get_properties()['B'], 3)
        self.assertEquals(props.get_properties()['C'], 4)
        props = FilePropertiesHandler(self.filename)
        self.assertFalse('A' in props.get_properties())
        self.assertEquals(props.get_properties()['B'], 3)
        self.assertEquals(props.get_properties()['C'], 4)
        props.update_properties({'C': 5})
        self.assertFalse('A' in props.get_properties())
        self.assertEquals(props.get_properties()['B'], 3)
        self.assertEquals(props.get_properties()['C'], 5)


if __name__ == '__main__':
    unittest.main()
