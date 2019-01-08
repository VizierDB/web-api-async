import os
import unittest
import shutil

from vizier.config.container import ContainerAppConfig, DEFAULT_SETTINGS, ENV_CONFIG

CONFIG_FILE = './.files/config-container.yaml'


class TestContainerConfig(unittest.TestCase):

    def test_default_config(self):
        """Test the default configuration settings."""
        config = ContainerAppConfig(configuration_file=CONFIG_FILE)
        self.assertEquals(config.webservice.server_url, 'http://vizier-db-container.info')
        self.assertEquals(config.webservice.server_port, 80)
        self.assertEquals(config.webservice.server_local_port, 80)
        self.assertEquals(config.webservice.app_path, DEFAULT_SETTINGS['webservice']['app_path'])
        self.assertEquals(config.webservice.doc_url, DEFAULT_SETTINGS['webservice']['doc_url'])
        self.assertEquals(config.webservice.name, DEFAULT_SETTINGS['webservice']['name'])
        self.assertEquals(config.app_base_url, 'http://vizier-db-container.info/vizier-db/api/v1')
        self.assertEquals(config.webservice.defaults.max_file_size, DEFAULT_SETTINGS['webservice']['defaults']['max_file_size'])
        self.assertEquals(config.webservice.defaults.row_limit,  DEFAULT_SETTINGS['webservice']['defaults']['row_limit'])
        self.assertEquals(config.webservice.defaults.max_row_limit,  DEFAULT_SETTINGS['webservice']['defaults']['max_row_limit'])
        self.assertEquals(config.debug, DEFAULT_SETTINGS['debug'])
        self.assertEquals(config.project_id, 'ABC')
        

if __name__ == '__main__':
    unittest.main()
