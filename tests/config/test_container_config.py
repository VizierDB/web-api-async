"""Test instantiating the container configuration object from the respective
environment variables.
"""

import os
import unittest

from vizier.config.container import ContainerConfig
from vizier.core.util import delete_env

import vizier.config.app as env
import vizier.config.container as container


class TestContainerConfig(unittest.TestCase):

    def setUp(self):
        """Clear all relevant environment variables."""
        # Test the default configuration. Ensure that no environment variable
        # is set.
        delete_env(env.VIZIERSERVER_NAME)
        delete_env(env.VIZIERSERVER_LOG_DIR)
        delete_env(env.VIZIERSERVER_DEBUG)
        delete_env(env.VIZIERSERVER_BASE_URL)
        delete_env(env.VIZIERSERVER_SERVER_PORT)
        delete_env(env.VIZIERSERVER_SERVER_LOCAL_PORT)
        delete_env(env.VIZIERSERVER_APP_PATH)
        delete_env(env.VIZIERSERVER_ROW_LIMIT)
        delete_env(env.VIZIERSERVER_MAX_ROW_LIMIT)
        delete_env(env.VIZIERSERVER_MAX_UPLOAD_SIZE)
        delete_env(env.VIZIERSERVER_ENGINE)
        delete_env(env.VIZIERSERVER_PACKAGE_PATH)
        delete_env(env.VIZIERSERVER_PROCESSOR_PATH)
        delete_env(container.VIZIERCONTAINER_PROJECT_ID)
        delete_env(container.VIZIERCONTAINER_CONTROLLER_URL)

    def test_default_config(self):
        """Test the default configuration settings."""
        with self.assertRaises(ValueError):
            config = ContainerConfig()
        os.environ[container.VIZIERCONTAINER_PROJECT_ID] = '000'
        with self.assertRaises(ValueError):
            config = ContainerConfig()
        os.environ[container.VIZIERCONTAINER_CONTROLLER_URL] = 'http://'
        config = ContainerConfig()
        self.assertEquals(config.webservice.name, env.DEFAULT_SETTINGS[env.VIZIERSERVER_NAME])
        self.assertEquals(config.webservice.server_url, env.DEFAULT_SETTINGS[env.VIZIERSERVER_BASE_URL])
        self.assertEquals(config.webservice.server_port, env.DEFAULT_SETTINGS[env.VIZIERSERVER_SERVER_PORT])
        self.assertEquals(config.webservice.server_local_port, env.DEFAULT_SETTINGS[env.VIZIERSERVER_SERVER_LOCAL_PORT])
        self.assertEquals(config.webservice.app_path, env.DEFAULT_SETTINGS[env.VIZIERSERVER_APP_PATH])
        self.assertEquals(config.webservice.defaults.row_limit, env.DEFAULT_SETTINGS[env.VIZIERSERVER_ROW_LIMIT])
        self.assertEquals(config.webservice.defaults.max_row_limit, env.DEFAULT_SETTINGS[env.VIZIERSERVER_MAX_ROW_LIMIT])
        self.assertEquals(config.webservice.defaults.max_file_size, env.DEFAULT_SETTINGS[env.VIZIERSERVER_MAX_UPLOAD_SIZE])
        self.assertEquals(config.run.debug, env.DEFAULT_SETTINGS[env.VIZIERSERVER_DEBUG])
        self.assertEquals(config.logs.server, env.DEFAULT_SETTINGS[env.VIZIERSERVER_LOG_DIR])
        self.assertEquals(config.engine.identifier, env.DEFAULT_SETTINGS[env.VIZIERSERVER_ENGINE])
        self.assertEquals(config.project_id, '000')
        self.assertEquals(config.controller_url, 'http://')

    def test_env_config(self):
        """Test app config using environment variables."""
        os.environ[container.VIZIERCONTAINER_PROJECT_ID] = '001'
        os.environ[container.VIZIERCONTAINER_CONTROLLER_URL] = 'http://localhost'
        os.environ[env.VIZIERSERVER_NAME] = 'Some Name'
        os.environ[env.VIZIERSERVER_LOG_DIR] = 'logdir'
        os.environ[env.VIZIERSERVER_DEBUG] = 'bla'
        os.environ[env.VIZIERSERVER_BASE_URL] = 'http://webapi'
        os.environ[env.VIZIERSERVER_SERVER_PORT] = '80'
        os.environ[env.VIZIERSERVER_SERVER_LOCAL_PORT] = '90'
        os.environ[env.VIZIERSERVER_APP_PATH] = 'vizier/v2'
        os.environ[env.VIZIERSERVER_ROW_LIMIT] = '111'
        os.environ[env.VIZIERSERVER_MAX_ROW_LIMIT] = '222'
        os.environ[env.VIZIERSERVER_MAX_UPLOAD_SIZE] = '333'
        os.environ[env.VIZIERSERVER_ENGINE] = 'CELERY'
        config = ContainerConfig()
        self.assertEquals(config.webservice.name, 'Some Name')
        self.assertEquals(config.webservice.server_url, 'http://webapi')
        self.assertEquals(config.webservice.server_port, 80)
        self.assertEquals(config.webservice.server_local_port, 90)
        self.assertEquals(config.webservice.app_path, 'vizier/v2')
        self.assertEquals(config.webservice.defaults.row_limit, 111)
        self.assertEquals(config.webservice.defaults.max_row_limit, 222)
        self.assertEquals(config.webservice.defaults.max_file_size, 333)
        self.assertEquals(config.run.debug, False)
        self.assertEquals(config.logs.server, 'logdir')
        self.assertEquals(config.engine.identifier, 'CELERY')
        self.assertEquals(config.project_id, '001')
        self.assertEquals(config.controller_url, 'http://localhost')


if __name__ == '__main__':
    unittest.main()
