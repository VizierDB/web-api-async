#! /usr/bin/env python

from setuptools import setup
import glob
import os

from setuptools import setup
from setuptools.command.develop import develop
from setuptools.command.install import install
from distutils.command.build import build
from subprocess import check_call
from wheel.bdist_wheel import bdist_wheel

def custom_command(command_subclass):
    orig_run = command_subclass.run
    
    def modified_run(self):
        print((str(command_subclass)))
        orig_run(self)
        
    command_subclass.run = modified_run
    return command_subclass

@custom_command
class CustomBuildCommand(build):
    pass

@custom_command
class CustomDevelopCommand(develop):
    pass

@custom_command
class CustomInstallCommand(install):
    pass

@custom_command
class CustomBdistWheelCommand(bdist_wheel):
    pass

def package_files(directory):
    paths = {}
    for (path, directories, filenames) in os.walk(directory):
        for filename in filenames:
            if path in paths:
                paths[path].append( os.path.join(path, filename))
            else:
                paths[path] = [os.path.join(path, filename)]
    return [(k.replace('../', ''), v) for k, v in list(paths.items())] 

packages_files = package_files('resources/packages/')
processors_files = package_files('resources/processors/')
webui_files = package_files('../web-ui/build/')
mimir_jar = [('mimir',['../mimir/bin/mimir-api'])]
data_files = packages_files + processors_files + webui_files + mimir_jar
#print(str(data_files))

setup(
    name='vizier-webapi',
    version='0.5.7',
    description='UI, Web API, and Backend for data curation projects and workflows',
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
    cmdclass={'bdist_wheel': CustomBdistWheelCommand, 
              'build': CustomBuildCommand, 
              'install': CustomInstallCommand, 
              'develop':CustomDevelopCommand},
)
