import os
import unittest

from vizier.config.app import AppConfig
from vizier.core.util import delete_env

import vizier.config.app as env


class TestAppConfig(unittest.TestCase):

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
        delete_env(env.VIZIERENGINE_DATA_DIR)
        delete_env(env.VIZIERENGINE_USE_SHORT_IDENTIFIER)
        delete_env(env.VIZIERENGINE_SYNCHRONOUS)
        delete_env(env.VIZIERENGINE_BACKEND)
        delete_env(env.VIZIERENGINE_CELERY_ROUTES)
        delete_env(env.VIZIERENGINE_CONTAINER_PORTS)
        delete_env(env.VIZIERENGINE_CONTAINER_IMAGE)

    def test_default_config(self):
        """Test the default configuration settings."""
        config = AppConfig()
        self.assertEqual(config.webservice.name, env.DEFAULT_SETTINGS[env.VIZIERSERVER_NAME])
        self.assertEqual(config.webservice.server_url, env.DEFAULT_SETTINGS[env.VIZIERSERVER_BASE_URL])
        self.assertEqual(config.webservice.server_port, env.DEFAULT_SETTINGS[env.VIZIERSERVER_SERVER_PORT])
        self.assertEqual(config.webservice.server_local_port, env.DEFAULT_SETTINGS[env.VIZIERSERVER_SERVER_LOCAL_PORT])
        self.assertEqual(config.webservice.app_path, env.DEFAULT_SETTINGS[env.VIZIERSERVER_APP_PATH])
        self.assertEqual(config.webservice.defaults.row_limit, env.DEFAULT_SETTINGS[env.VIZIERSERVER_ROW_LIMIT])
        self.assertEqual(config.webservice.defaults.max_row_limit, env.DEFAULT_SETTINGS[env.VIZIERSERVER_MAX_ROW_LIMIT])
        self.assertEqual(config.webservice.defaults.max_file_size, env.DEFAULT_SETTINGS[env.VIZIERSERVER_MAX_UPLOAD_SIZE])
        self.assertEqual(config.run.debug, env.DEFAULT_SETTINGS[env.VIZIERSERVER_DEBUG])
        self.assertEqual(config.logs.server, env.DEFAULT_SETTINGS[env.VIZIERSERVER_LOG_DIR])
        self.assertEqual(config.engine.identifier, env.DEFAULT_SETTINGS[env.VIZIERSERVER_ENGINE])
        self.assertEqual(config.engine.data_dir, env.DEFAULT_SETTINGS[env.VIZIERENGINE_DATA_DIR])
        self.assertEqual(config.engine.package_path, env.DEFAULT_SETTINGS[env.VIZIERSERVER_PACKAGE_PATH])
        self.assertEqual(config.engine.processor_path, env.DEFAULT_SETTINGS[env.VIZIERSERVER_PROCESSOR_PATH])
        self.assertEqual(config.engine.use_short_ids, env.DEFAULT_SETTINGS[env.VIZIERENGINE_USE_SHORT_IDENTIFIER])
        self.assertEqual(config.engine.sync_commands, env.DEFAULT_SETTINGS[env.VIZIERENGINE_SYNCHRONOUS])
        self.assertEqual(config.engine.backend.identifier, env.DEFAULT_SETTINGS[env.VIZIERENGINE_BACKEND])
        self.assertEqual(config.engine.backend.celery.routes, env.DEFAULT_SETTINGS[env.VIZIERENGINE_CELERY_ROUTES])
        self.assertEqual(config.engine.backend.container.ports, env.DEFAULT_SETTINGS[env.VIZIERENGINE_CONTAINER_PORTS])
        self.assertEqual(config.engine.backend.container.image, env.DEFAULT_SETTINGS[env.VIZIERENGINE_CONTAINER_IMAGE])

    def test_env_config(self):
        """Test app config using environment variables."""
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
        os.environ[env.VIZIERENGINE_USE_SHORT_IDENTIFIER] = str(not env.DEFAULT_SETTINGS[env.VIZIERENGINE_USE_SHORT_IDENTIFIER])
        os.environ[env.VIZIERENGINE_SYNCHRONOUS] = 'ABC'
        os.environ[env.VIZIERENGINE_BACKEND] = 'THE_BACKEND'
        os.environ[env.VIZIERENGINE_CELERY_ROUTES] = 'Some Routes'
        os.environ[env.VIZIERENGINE_CONTAINER_PORTS] = '8080-8084,9000,10001-10010'
        config = AppConfig()
        self.assertEqual(config.webservice.name, 'Some Name')
        self.assertEqual(config.webservice.server_url, 'http://webapi')
        self.assertEqual(config.webservice.server_port, 80)
        self.assertEqual(config.webservice.server_local_port, 90)
        self.assertEqual(config.webservice.app_path, 'vizier/v2')
        self.assertEqual(config.webservice.defaults.row_limit, 111)
        self.assertEqual(config.webservice.defaults.max_row_limit, 222)
        self.assertEqual(config.webservice.defaults.max_file_size, 333)
        self.assertEqual(config.run.debug, False)
        self.assertEqual(config.logs.server, 'logdir')
        self.assertEqual(config.engine.identifier, 'CELERY')
        self.assertEqual(config.engine.use_short_ids, not env.DEFAULT_SETTINGS[env.VIZIERENGINE_USE_SHORT_IDENTIFIER])
        self.assertEqual(config.engine.sync_commands, 'ABC')
        self.assertEqual(config.engine.backend.identifier, 'THE_BACKEND')
        self.assertEqual(config.engine.backend.celery.routes, 'Some Routes')
        ports = list(range(8080, 8084)) + [9000] + list(range(10001,10010))
        self.assertEqual(config.engine.backend.container.ports, ports)
        self.assertEqual(config.engine.backend.container.image, env.DEFAULT_SETTINGS[env.VIZIERENGINE_CONTAINER_IMAGE])


if __name__ == '__main__':
    unittest.main()
