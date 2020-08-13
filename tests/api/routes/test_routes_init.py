"""Test initialization of the url factory classes"""

import unittest

from vizier.api.routes.base import UrlFactory
from vizier.api.routes.base import PROPERTIES_BASEURL, PROPERTIES_APIDOCURL
from vizier.api.routes.task import TaskUrlFactory


class TestUrlFactoryInit(unittest.TestCase):

    def test_init_url_factory(self):
        """Test initializing the main url factory."""
        urls = UrlFactory(base_url='http://abc.com/////')
        self.assertEqual(urls.base_url, 'http://abc.com')
        self.assertIsNone(urls.api_doc_url)
        urls = UrlFactory(base_url='http://abc.com/////', api_doc_url='ABC')
        self.assertEqual(urls.base_url, 'http://abc.com')
        self.assertEqual(urls.api_doc_url, 'ABC')
        # Override API doc url via properties
        urls = UrlFactory(
            base_url='http://abc.com/////',
            api_doc_url='ABC',
            properties={PROPERTIES_APIDOCURL: 'XYZ'}
        )
        self.assertEqual(urls.base_url, 'http://abc.com')
        self.assertEqual(urls.api_doc_url, 'XYZ')
        # Override base url via properties
        urls = UrlFactory(
            base_url='http://abc.com/////',
            api_doc_url='ABC',
            properties={PROPERTIES_BASEURL: 'XYZ'}
        )
        self.assertEqual(urls.base_url, 'XYZ')
        self.assertEqual(urls.api_doc_url, 'ABC')
        # Initialize only via properties
        urls = UrlFactory(properties={
            PROPERTIES_BASEURL: 'XYZ',
            PROPERTIES_APIDOCURL: 'ABC'
        })
        self.assertEqual(urls.base_url, 'XYZ')
        self.assertEqual(urls.api_doc_url, 'ABC')
        # Value error if base url is not set
        with self.assertRaises(AssertionError):
            urls = UrlFactory(
                api_doc_url='ABC',
                properties={PROPERTIES_APIDOCURL: 'XYZ'}
            )

    def test_tasks_url_factory(self):
        """Initialize the task url factory."""
        fact = TaskUrlFactory(base_url='http://abc.com/////')
        self.assertEqual(fact.base_url, 'http://abc.com')
        self.assertEqual(fact.set_task_state(task_id='TID'), 'http://abc.com/tasks/TID')
        # Initialize from class loader
        fact = TaskUrlFactory(
            base_url='http://abc.com/////',
            properties={PROPERTIES_BASEURL: 'XYZ'}
        )
        self.assertEqual(fact.base_url, 'XYZ')
        # Value error is no url factory is given
        with self.assertRaises(ValueError):
            TaskUrlFactory()

if __name__ == '__main__':
    unittest.main()
