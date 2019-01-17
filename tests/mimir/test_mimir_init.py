"""Test initializing and shutting down the mimir gateway."""

import unittest

import vizier.mimir as mimir


class TestMimirInit(unittest.TestCase):
    """Simple test cases to ensure that the Mimir gateway can be initialized
    and shutdown without errors.
    """
    def testInit(self):
        """Initialize Mimir gateway and shutdown."""
        mimir.initialize()
        self.assertIsNotNone(mimir._mimir)
        self.assertIsNotNone(mimir._gateway)
        self.assertIsNotNone(mimir._jvmhelper)
        mimir.finalize()


if __name__ == '__main__':
    unittest.main()
