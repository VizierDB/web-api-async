"""Test the class loader."""

import os
import shutil
import unittest

from vizier.core.loader import ClassLoader


class TestClassLoader(unittest.TestCase):

    def test_create_instance(self):
        """Test creating an instance from a class loader."""
        # Create instance of plot processor
        loader = ClassLoader(
            values=ClassLoader.to_dict(
                module_name='vizier.engine.packages.plot.processor',
                class_name='PlotProcessor'
            )
        )
        engine = loader.get_instance()
        from vizier.engine.packages.plot.processor import PlotProcessor
        self.assertTrue(isinstance(engine, PlotProcessor))
        with self.assertRaises(ValueError):
            engine.compute(command_id='ABC', arguments=None, context=None)
        loader = ClassLoader(
            values=ClassLoader.to_dict(
                module_name='vizier.core.loader',
                class_name='DummyClass',
                properties={'A': 1}
            )
        )
        dummy = loader.get_instance(names=['X', 'Y', 'Z'])
        self.assertEquals(dummy.properties['A'], 1)
        self.assertEquals(dummy.names, ['X', 'Y', 'Z'])

    def test_serialize_and_create(self):
        """Test creating a class loader instance from a dictionary."""
        loader = ClassLoader(
            values=ClassLoader.to_dict(
                module_name='ABC',
                class_name='DEF',
                properties={'A': 1}
            )
        )
        self.assertEquals(loader.module_name, 'ABC')
        self.assertEquals(loader.class_name, 'DEF')
        self.assertEquals(loader.properties['A'], 1)
        # No properties given
        loader = ClassLoader(
            values=ClassLoader.to_dict(
                module_name='ABC',
                class_name='DEF'
            )
        )
        self.assertEquals(loader.module_name, 'ABC')
        self.assertEquals(loader.class_name, 'DEF')
        self.assertIsNone(loader.properties)
        # Errors for invalid dictionaries
        values = ClassLoader.to_dict(
            module_name='ABC',
            class_name='DEF',
            properties={'A': 1}
        )
        del values['moduleName']
        with self.assertRaises(ValueError):
            ClassLoader(values=values)
        values = ClassLoader.to_dict(
            module_name='ABC',
            class_name='DEF',
            properties={'A': 1}
        )
        del values['className']
        with self.assertRaises(ValueError):
            ClassLoader(values=values)


if __name__ == '__main__':
    unittest.main()
