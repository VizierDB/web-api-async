"""Ensure that the module timestamp object is initialized properly."""
import unittest

from vizier.viztrail.module.timestamp import ModuleTimestamp


class TestModuleTimestamp(unittest.TestCase):

    def test_init(self):
        """Ensure that the module timestamp object is initialized properly."""
        # Ensure that the created_at timestamp is set if not given
        ts = ModuleTimestamp()
        self.assertIsNotNone(ts.created_at)
        self.assertIsNone(ts.started_at)
        self.assertIsNone(ts.finished_at)
        created_at = ts.created_at
        # Ensure created_at and started_at are initialize properly
        ts = ModuleTimestamp(created_at=created_at, started_at=created_at)
        self.assertIsNotNone(ts.created_at)
        self.assertIsNotNone(ts.started_at)
        self.assertIsNone(ts.finished_at)
        self.assertEqual(ts.created_at, created_at)
        self.assertEqual(ts.started_at, created_at)
        # Ensure that ValueError is raised if created_at is None but one of
        # the other two timestamp arguments is not
        with self.assertRaises(ValueError):
            ModuleTimestamp(started_at=created_at)
        with self.assertRaises(ValueError):
            ModuleTimestamp(finished_at=created_at)
        with self.assertRaises(ValueError):
            ModuleTimestamp(started_at=created_at, finished_at=created_at)


if __name__ == '__main__':
    unittest.main()
