import os
import unittest

from vizier.config.worker import WorkerConfig
from vizier.core.util import delete_env

import vizier.config.worker as env


class TestWorkerConfig(unittest.TestCase):

    def setUp(self):
        """Clear all relevant environment variables."""
        # Test the default configuration. Ensure that no environment variable
        # is set.
        delete_env(env.VIZIERWORKER_LOG_DIR)
        delete_env(env.VIZIERWORKER_CONTROLLER_URL)
        delete_env(env.VIZIERWORKER_ENV)
        delete_env(env.VIZIERWORKER_PROCESSOR_PATH)

    def test_default_config(self):
        """Test the default worker configuration settings."""
        config = WorkerConfig()
        self.assertEqual(config.controller.url, env.DEFAULT_SETTINGS[env.VIZIERWORKER_CONTROLLER_URL])
        self.assertEqual(config.env.identifier, env.DEFAULT_SETTINGS[env.VIZIERWORKER_ENV])
        self.assertEqual(config.env.processor_path, env.DEFAULT_SETTINGS[env.VIZIERWORKER_PROCESSOR_PATH])
        self.assertEqual(config.logs.worker, env.DEFAULT_SETTINGS[env.VIZIERWORKER_LOG_DIR])

    def test_env_config(self):
        """Test worker config using environment variables."""
        os.environ[env.VIZIERWORKER_CONTROLLER_URL] = 'http://webapi'
        os.environ[env.VIZIERWORKER_ENV] = 'REMOTE'
        os.environ[env.VIZIERWORKER_PROCESSOR_PATH] = 'processors'
        os.environ[env.VIZIERWORKER_LOG_DIR] = 'logdir'
        config = WorkerConfig()
        self.assertEqual(config.controller.url, 'http://webapi')
        self.assertEqual(config.env.identifier, 'REMOTE')
        self.assertEqual(config.env.processor_path, 'processors')
        self.assertEqual(config.logs.worker, 'logdir')


if __name__ == '__main__':
    unittest.main()
