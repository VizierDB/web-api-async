"""Ensure that the module output object is working properly."""
import unittest

from vizier.viztrail.module.output import ModuleOutputs, OutputObject, TextOutput


class TestModuleOutput(unittest.TestCase):

    def test_init(self):
        """test getter and setter methods for output streams."""
        # Ensure that lists are initialized properly
        out = ModuleOutputs()
        self.assertEqual(len(out.stderr), 0)
        self.assertEqual(len(out.stdout), 0)
        out.stdout.append(TextOutput(value='Hello World'))
        out.stderr.append(OutputObject(type='ERROR', value='Some Error'))
        out = ModuleOutputs(stdout=out.stdout, stderr=out.stderr)
        self.assertEqual(len(out.stderr), 1)
        self.assertEqual(len(out.stdout), 1)
        self.assertTrue(out.stdout[0].is_text)
        self.assertFalse(out.stderr[0].is_text)


if __name__ == '__main__':
    unittest.main()
