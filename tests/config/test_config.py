import unittest

from vizier.config import AppConfig
from vizier.config import DEFAULT_ENV_ID, DEFAULT_ENV_NAME, DEFAULT_ENV_DESC
from vizier.engine.packages.sys import PACKAGE_SYS
from vizier.engine.packages.mimir.base import PACKAGE_MIMIR
from vizier.engine.packages.plot.base import PACKAGE_PLOT
from vizier.engine.packages.pycell.base import PACKAGE_PYTHON
from vizier.engine.packages.vizual.base import PACKAGE_VIZUAL

CONFIG_FILE = './.files/config.yaml'
ALTERNATE_CONFIG_FILE = './.files/alternate-config.yaml'
ERRONEOUS_CONFIG_FILE = './.files/erroneous-config.yaml'
DUPLICATE_ENV_CONFIG_FILE = './.files/dup-env-config.yaml'
MISSING_PACKAGE_CONFIG_FILE = './.files/missing-pckg-config.yaml'
SYS_PACKAGE_CONFIG_FILE = './.files/sys-pckg-config.yaml'


class TestConfig(unittest.TestCase):

    def test_default_config(self):
        """Test the default configuration settings.
        """
        config = AppConfig(configuration_file=CONFIG_FILE)
        # Packages
        for pckg_key in [PACKAGE_MIMIR, PACKAGE_PLOT, PACKAGE_PYTHON, PACKAGE_VIZUAL, PACKAGE_SYS]:
            self.assertTrue(pckg_key in config.packages)
        # Settings
        settings = config.settings
        # API
        self.assertEquals(settings.api.server_url, 'http://vizier-db.info')
        self.assertEquals(settings.api.server_port, 80)
        self.assertEquals(settings.api.server_local_port, 5000)
        self.assertEquals(settings.api.app_path, '/vizier-db/api/v2')
        self.assertEquals(settings.api.doc_url, 'http://vizier-db.info/doc/api/v1')
        self.assertEquals(config.app_base_url, 'http://vizier-db.info/vizier-db/api/v2')
        # File server
        self.assertEquals(settings.fileserver.directory, 'fs-directory')
        self.assertEquals(settings.fileserver.max_file_size, 1024)
        # Viztrails
        self.assertEquals(settings.viztrails.directory, '../.env/vt')
        # Defaults
        self.assertEquals(settings.defaults.row_limit, 25)
        self.assertEquals(settings.defaults.max_row_limit, -1)
        # Env
        self.assertEquals(len(config.envs), 1)
        env = config.envs[DEFAULT_ENV_ID]
        self.assertEquals(env.identifier, DEFAULT_ENV_ID)
        self.assertEquals(env.name, DEFAULT_ENV_NAME)
        self.assertEquals(env.description, DEFAULT_ENV_DESC)
        self.assertEquals(env.datastore.properties.directory, '../.env/ds')
        # Misc
        self.assertEquals(settings.name, 'Vizier Web API')
        self.assertEquals(settings.debug, True)
        # Logs
        self.assertEquals(settings.logs.server, 'logs')
        self.assertEquals(settings.logs.engine, False)
        # Make sure that the configuration file is found if no parameter is
        # given
        config = AppConfig()
        # Packages
        for pckg_key in [PACKAGE_MIMIR, PACKAGE_PLOT, PACKAGE_PYTHON, PACKAGE_VIZUAL, PACKAGE_SYS]:
            self.assertTrue(pckg_key in config.packages)
        # Settings
        settings = config.settings
        # API
        self.assertEquals(settings.api.server_url, 'http://vizier-db.info')
        self.assertEquals(settings.api.server_port, 80)
        self.assertEquals(settings.api.server_local_port, 5000)
        self.assertEquals(settings.api.app_path, '/vizier-db/api/v2')
        self.assertEquals(settings.api.doc_url, 'http://vizier-db.info/doc/api/v1')
        self.assertEquals(config.app_base_url, 'http://vizier-db.info/vizier-db/api/v2')
        # File server
        self.assertEquals(settings.fileserver.directory, 'fs-directory')
        self.assertEquals(settings.fileserver.max_file_size, 1024)
        # Viztrails
        self.assertEquals(settings.viztrails.directory, '../.env/vt')
        # Defaults
        self.assertEquals(settings.defaults.row_limit, 25)
        self.assertEquals(settings.defaults.max_row_limit, -1)
        # Env
        self.assertEquals(len(config.envs), 1)
        env = config.envs[DEFAULT_ENV_ID]
        self.assertEquals(env.identifier, DEFAULT_ENV_ID)
        self.assertEquals(env.name, DEFAULT_ENV_NAME)
        self.assertEquals(env.description, DEFAULT_ENV_DESC)
        self.assertEquals(env.datastore.properties.directory, '../.env/ds')
        # Misc
        self.assertEquals(settings.name, 'Vizier Web API')
        self.assertEquals(settings.debug, True)
        # Logs
        self.assertEquals(settings.logs.server, 'logs')
        self.assertEquals(settings.logs.engine, False)

    def test_envs_config(self):
        """Test reading configuration with environment specification.
        """
        config = AppConfig(configuration_file=ALTERNATE_CONFIG_FILE)
        # Env
        self.assertEquals(len(config.envs), 2)
        env = config.envs['MIMIR']
        self.assertEquals(env.identifier, 'MIMIR')
        self.assertEquals(env.name, 'Mimir Config')
        self.assertEquals(env.description, 'Mimir Engine')
        self.assertEquals(env.datastore.properties.directory, 'mimir-directory')
        self.assertEquals(len(env.packages), 3)
        self.assertTrue('python' in env.packages)
        self.assertTrue('vizual' in env.packages)
        self.assertTrue('mimir' in env.packages)
        env = config.envs['DEFAULT']
        self.assertEquals(env.identifier, 'DEFAULT')
        self.assertEquals(env.name, 'Default Config')
        self.assertEquals(env.description, 'Default Engine')
        self.assertEquals(env.datastore.properties.directory, 'default-directory')
        self.assertEquals(len(env.packages), 2)
        self.assertTrue('python' in env.packages)
        self.assertTrue('vizual' in env.packages)

    def test_duplicate_env_config(self):
        """Test reading configuration with duplicate environemnt definition.
        """
	with self.assertRaises(ValueError):
        	config = AppConfig(configuration_file=DUPLICATE_ENV_CONFIG_FILE)

    def test_erroneous_config(self):
        """Test reading configuration with invalid environment object.
        """
	with self.assertRaises(ValueError):
        	config = AppConfig(configuration_file=ERRONEOUS_CONFIG_FILE)

    def test_missing_package_config(self):
        """Test reading configuration with unknown package name in environment
        object.
        """
	with self.assertRaises(ValueError):
        	config = AppConfig(configuration_file=MISSING_PACKAGE_CONFIG_FILE)

    def test_sys_package_config(self):
        """Test reading configuration that specifies the system package in the
        environment's package list.
        """
	with self.assertRaises(ValueError):
        	config = AppConfig(configuration_file=SYS_PACKAGE_CONFIG_FILE)


if __name__ == '__main__':
    unittest.main()
