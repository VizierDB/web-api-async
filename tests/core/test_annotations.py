import os
import shutil
import unittest

from vizier.core.annotation.base import AnnotationStore, DefaultAnnotationSet


class TestStore(AnnotationStore):
    """Simple annotation store to test whether the store function is called from
    the default annotation set.
    """
    def __init__(self):
        self.called = False

    def store(self, annotations):
        self.called = True


class TestDefaultAnnotationSet(unittest.TestCase):

    def test_default_values(self):
        """Test functionality of default values for find methods in annotation
        sets.
        """
        annotations = DefaultAnnotationSet()
        annotations.add('A', 1)
        # Test default values
        self.assertIsNone(annotations.find_one('B'))
        self.assertEquals(annotations.find_one('B', default_value='XYZ'), 'XYZ')
        self.assertEquals(annotations.find_all('B'), [])
        self.assertEquals(annotations.find_all('B', default_value=[1]), [1])
        # Set value for 'B'
        annotations.add('B', 'abc')
        self.assertEquals(annotations.find_one('B'), 'abc')
        self.assertEquals(annotations.find_one('B', default_value='XYZ'), 'abc')
        self.assertEquals(annotations.find_all('B'), ['abc'])
        self.assertEquals(annotations.find_all('B', default_value=[1]), ['abc'])
        annotations = DefaultAnnotationSet(elements={'A': 1})
        self.assertIsNone(annotations.find_one('B'))
        self.assertEquals(annotations.find_one('A'), 1)
        annotations = DefaultAnnotationSet(elements={'A': [1, 'XYZ']})
        self.assertIsNone(annotations.find_one('B'))
        self.assertIsNotNone(annotations.find_one('A', raise_error_on_multi_value=False))
        self.assertTrue(1 in annotations.find_all('A'))
        self.assertTrue('XYZ' in annotations.find_all('A'))

    def test_delete_values(self):
        """Test functionality that deletes annotations.
        """
        annotations = DefaultAnnotationSet()
        annotations.add('A', 1)
        self.assertFalse(annotations.delete('A', value=2))
        self.assertTrue(annotations.delete('A', value=1))
        self.assertFalse(annotations.delete('A', value=1))
        self.assertFalse(annotations.delete('A'))
        annotations = DefaultAnnotationSet(elements={'A': 1})
        self.assertTrue(annotations.delete('A', value=1))
        self.assertFalse(annotations.delete('A', value=1))
        self.assertFalse(annotations.delete('A'))
        annotations = DefaultAnnotationSet(elements={'A': [1, 2]})
        self.assertTrue(annotations.delete('A', value=1))
        self.assertFalse(annotations.delete('A', value=1))
        self.assertTrue(annotations.delete('A'))
        self.assertFalse(annotations.delete('A'))

    def test_multi_value_exception(self):
        """Test functionality that throws exception for find_one query."""
        annotations = DefaultAnnotationSet()
        annotations.add('A', 1)
        # Test default values
        self.assertEquals(annotations.find_one('A'), 1)
        annotations.add('A', 1)
        self.assertEquals(annotations.find_one('A'), 1)
        annotations.add('A', 2)
        with self.assertRaises(ValueError):
            self.assertEquals(annotations.find_one('A'), 1)
        self.assertIsNotNone(annotations.find_one('A', raise_error_on_multi_value=False))

    def test_persistent_store(self):
        """Test calls to persistent annotation store."""
        # Add
        annotations = DefaultAnnotationSet(writer=TestStore())
        annotations.add('A', 1)
        self.assertTrue(annotations.writer.called)
        annotations.writer.called = False
        annotations.add('A', 1)
        self.assertFalse(annotations.writer.called)
        annotations.add('A', 2)
        self.assertTrue(annotations.writer.called)
        annotations.writer.called = False
        annotations.add('A', 2)
        self.assertFalse(annotations.writer.called)
        annotations.writer.called = False
        annotations.replace('A', 3)
        self.assertTrue(annotations.writer.called)
        annotations.writer.called = False
        annotations.replace('A', 3)
        self.assertFalse(annotations.writer.called)
        # Delete
        annotations = DefaultAnnotationSet(
            elements={'A': [1, 2], 'B': 'XYZ'},
            writer=TestStore()
        )
        self.assertFalse(annotations.writer.called)
        annotations.delete('C')
        self.assertFalse(annotations.writer.called)
        annotations.delete('A', value='1')
        self.assertFalse(annotations.writer.called)
        annotations.delete('A', value=2)
        self.assertTrue(annotations.writer.called)
        annotations.writer.called = False
        annotations.delete('A', value=2)
        self.assertFalse(annotations.writer.called)
        annotations.delete('A', value=1)
        self.assertTrue(annotations.writer.called)

    def test_replace_values(self):
        """Test replace values functionality."""
        annotations = DefaultAnnotationSet()
        annotations.add('A', 1)
        annotations.add('A', 2)
        val = annotations.find_all('A')
        self.assertTrue(isinstance(val, list))
        annotations.replace('A', 1)
        val = annotations.find_all('A')
        self.assertTrue(isinstance(val, list))
        self.assertEquals(val, [1])
        annotations.add('A', 2)
        val = annotations.find_all('A')
        self.assertTrue(isinstance(val, list))
        self.assertEquals(len(val), 2)
        annotations.replace('A', 2)
        val = annotations.find_one('A')
        self.assertEquals(val, 2)

    def test_update_empty_set(self):
        """Test functionality that adds to an empty annotation set."""
        # Create an empty properties file
        annotations = DefaultAnnotationSet()
        annotations.add('A', 1)
        # Both find_one and find_all (with or without exception flag) should
        # return 1
        self.assertEquals(annotations.find_one('A'), 1)
        self.assertEquals(annotations.find_one('A', raise_error_on_multi_value=False), 1)
        self.assertEquals(annotations.find_one('A', raise_error_on_multi_value=True), 1)
        # Find all will always return a list of elements
        self.assertEquals(annotations.find_all('A'), [1])
        annotations.add('A', '2')
        # The result of find_all is a list with two elements
        val = annotations.find_all('A')
        self.assertTrue(isinstance(val, list))
        self.assertEquals(len(val), 2)
        self.assertTrue(1 in val)
        self.assertTrue('2' in val)
        # Adding a duplicate annotation should not change anything
        annotations.add('A', '2')
        # The result of find_all is a list with two elements
        val = annotations.find_all('A')
        self.assertTrue(isinstance(val, list))
        self.assertEquals(len(val), 2)
        self.assertTrue(1 in val)
        self.assertTrue('2' in val)


if __name__ == '__main__':
    unittest.main()
