#! /usr/bin/env python

from setuptools import setup

setup(
    name='vizier-webapi',
    version='0.5.0',
    description='API to query and manipulate Vizier DB data curation projects and workflows',
    keywords='data curation ',
    license='apache-2.0',
    packages=['vizier'],
    package_data={'': ['LICENSE.txt']},
    install_requires=[
	'Flask>=1.0',
	'flask-cors',
    'celery',
    'docker',
    'jsonschema',
	'pyyaml',
	'py4j>=0.10.6',
    'requests',
	'spylon>=0.3.0',
    'unicodecsv>=0.14.1'
    ]
)
