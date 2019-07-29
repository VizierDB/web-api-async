#! /usr/bin/env python

from setuptools import setup
import glob
import os


def package_files(directory):
    paths = {}
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if path in paths:
                paths[path].append( os.path.join(path, filename))
            else:
                paths[path] = [os.path.join(path, filename)]
    return [(k.replace('../', ''), v) for k, v in paths.items()] 

packages_files = package_files('resources/packages/')
processors_files = package_files('resources/processors/')
webui_files = package_files('../web-ui/build/')
mimir_jar = [('mimir',['../mimir/bin/mimir-api'])]
data_files = packages_files + processors_files + webui_files + mimir_jar
print(str(data_files))

setup(
    name='vizier-webapi',
    version='0.5.5',
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
        'requests',
        'bokeh',
        'unicodecsv>=0.14.1'
    ],
    include_package_data=True,
    data_files=data_files,
    scripts=['tools/vizier'],
)
