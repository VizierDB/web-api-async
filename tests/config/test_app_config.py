import os
import unittest
import shutil

from vizier.config.app import AppConfig, DEFAULT_SETTINGS, ENV_CONFIG

CONFIG_FILE = './.files/config.yaml'
CONFIG_INCOMPLETE = './.files/incomplete_config.yaml'
CONFIG_WITH_ERRORS = '.files/config_with_errors.yaml'


class TestConfig(unittest.TestCase):

    def test_default_config(self):
        """Test the default configuration settings.
        """
        # Test the default configuration if not configuration file is given.
        # Ensure that the environment variable VIZIERSERVER_CONFIG is not set.
        config = AppConfig(configuration_file=CONFIG_FILE)
        self.assertEquals(config.webservice.server_url, 'http://vizier-db.info')
        self.assertEquals(config.webservice.server_port, DEFAULT_SETTINGS['webservice']['server_port'])
        self.assertEquals(config.webservice.server_local_port, 80)
        self.assertEquals(config.webservice.app_path, '/vizier-db/api/v2')
        self.assertEquals(config.webservice.doc_url, 'http://vizier-db.info/doc/api/v1')
        self.assertEquals(config.webservice.name, 'Vizier Web API')
        self.assertEquals(config.app_base_url, 'http://vizier-db.info:5000/vizier-db/api/v2')
        self.assertEquals(config.webservice.defaults.max_file_size, 1024)
        self.assertEquals(config.webservice.defaults.row_limit, 25)
        self.assertEquals(config.webservice.defaults.max_row_limit, -1)
        self.assertEquals(config.debug, True)
        self.assertEquals(config.logs.server, 'logs')

    def test_config_with_error(self):
        """Test loading configuration file with erroneous package specification."""
        with self.assertRaises(ValueError):
            AppConfig(configuration_file=CONFIG_INCOMPLETE)
        with self.assertRaises(ValueError):
            AppConfig(configuration_file=CONFIG_WITH_ERRORS)


if __name__ == '__main__':
    unittest.main()
