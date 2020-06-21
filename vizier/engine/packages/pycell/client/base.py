# Copyright (C) 2017-2019 New York University,
#                         University at Buffalo,
#                         Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""The vizier datastore client enables access to and manipulation of datasets in
a datastore from within a python script.
"""

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.datastore.annotation.dataset import DatasetMetadata
from vizier.datastore.object.base import PYTHON_EXPORT_TYPE
from vizier.datastore.object.dataobject import DataObjectMetadata
from vizier.engine.packages.pycell.client.dataset import DatasetClient
from os.path import normpath, basename
from os import path
import os
import re
import ast
import astor
import inspect

class VizierDBClient(object):
    """The Vizier DB Client provides access to datasets that are identified by
    a unique name. The client is a wrapper around a given database state.
    """
    def __init__(self, datastore, datasets, source, dataobjects):
        """Initialize the reference to the workflow context and the datastore.

        Parameters
        ----------
        datastore: vizier.datastore.base.Datastore
            Data store to access and manipulate datasets
        datasets: dict
            Mapping of dataset names to unique persistent dataset identifier
            generated by the data store.
        """
        self.datastore = datastore
        self.datasets = dict(datasets)
        self.dataobjects = dict(dataobjects)
        self.source = source
        # Keep track of the descriptors of datasets that the client successfully
        # modified
        self.descriptors = dict()
        # Keep track of datasets that are read and written, deleted and renamed.
        self.read = set()
        self.write = set()
        self.delete = None

    def export_module_decorator(self, original_func):
        def wrapper(*args, **kwargs):
            self.read.add(original_func.__name__)
            result = original_func(*args, **kwargs)
            return result
        return wrapper

    def wrap_variable(self, original_variable, name):
        self.read.add(name)
        return original_variable

    def export_module(self, exp):
        if inspect.isclass(exp):
            exp_name = exp.__name__
        elif callable(exp):
            exp_name = exp.__name__
        else:
            # If its a variable we grab the original name from the stack
            lcls = inspect.stack()[1][0].f_locals
            for name in lcls:
                if lcls[name] == exp:
                    exp_name = name
        src_ast = ast.parse(self.source)
        analyzer = Analyzer(exp_name)
        analyzer.visit(src_ast)
        src = analyzer.get_Source()
        if exp_name in self.dataobjects.keys():
            src_identifier = self.dataobjects[exp_name]
        else:
            src_identifier = get_unique_identifier()

        self.datastore.update_object(identifier=src_identifier,
                                     key=exp_name,
                                     new_value=src,
                                     obj_type=PYTHON_EXPORT_TYPE)
        self.set_dataobject_identifier(exp_name, src_identifier)
        self.descriptors[src_identifier] = self.datastore.get_objects(identifier=src_identifier).objects[0]

    def get_dataobject_identifier(self, name):
        """Returns the unique identifier for the dataset with the given name.

        Raises ValueError if no dataset with the given name exists.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        string
        """
        # Datset names should be case insensitive
        key = name
        if not key in self.dataobjects:
            raise ValueError('unknown dataobject \'' + name + '\'')
        return self.dataobjects[key]

    def set_dataobject_identifier(self, name, identifier):
        """Sets the identifier to which the given dataset name points.

        Parameters
        ----------
        name: string
            Dataset name
        identifier: string
            Unique identifier for persistent dataset
        """
        # Convert name to lower case to ensure that names are case insensitive
        self.dataobjects[name] = identifier
        self.write.add(name)

    def create_dataset(self, name, dataset, backend_options = []):
        """Create a new dataset with given name.

        Raises ValueError if a dataset with given name already exist.

        Parameters
        ----------
        name : string
            Unique dataset name
        dataset : vizier.datastore.client.DatasetClient
            Dataset object

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        # Raise an exception if a dataset with the given name already exists or
        # if the name is not valid
        if self.has_dataset_identifier(name):
            # Record access to the datasets
            self.read.add(name.lower())
            raise ValueError('dataset \'' + name + '\' already exists')
        if not is_valid_name(name):
            raise ValueError('invalid dataset name \'' + name + '\'')
        # Create list of columns for new dataset. Ensure that every column has
        # a positive identifier
        columns = list()
        if len(dataset.columns) > 0:
            column_counter = max(max([col.identifier for col in dataset.columns]) + 1, 0)
            for col in dataset.columns:
                if col.identifier < 0:
                    col.identifier = column_counter
                    column_counter += 1
                columns.append(
                    DatasetColumn(
                        identifier=col.identifier,
                        name=col.name,
                        data_type=col.data_type
                    )
                )
        rows = dataset.rows
        if len(rows) > 0:
            # Ensure that all rows have positive identifier
            row_counter = max(max([row.identifier for row in rows]) + 1, 0)
            for row in rows:
                if row.identifier < 0:
                    row.identifier = row_counter
                    row_counter += 1
        # Write dataset to datastore and add new dataset to context
        ds = self.datastore.create_dataset(
            columns=columns,
            rows=rows,
            annotations=dataset.annotations,
            human_readable_name=name.upper(),
            backend_options=backend_options
        )
        self.set_dataset_identifier(name, ds.identifier)
        self.descriptors[ds.identifier] = ds
        return DatasetClient(dataset=self.datastore.get_dataset(ds.identifier))

    def drop_dataset(self, name):
        """Remove the dataset with the given name.

        Raises ValueError if no dataset with given name exist.

        Parameters
        ----------
        name : string
            Unique dataset name
        """
        # Make sure to record access idependently of whether the dataset exists
        # or not. Ignore read access to datasets that have been written.
        if not name.lower() in self.write:
            self.read.add(name.lower())
        # Remove the context dataset identifier for the given name. Will raise
        # a ValueError if dataset does not exist
        if self.delete is None:
            self.delete = set()
        self.delete.add(name)
        self.remove_dataset_identifier(name)

    def get_dataset(self, name):
        """Get dataset with given name.

        Raises ValueError if the specified dataset does not exist.

        Parameters
        ----------
        name : string
            Unique dataset name

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        # Make sure to record access idependently of whether the dataset exists
        # or not. Ignore read access to datasets that have been written.
        if not name.lower() in self.write:
            self.read.add(name.lower())
        # Get identifier for the dataset with the given name. Will raise an
        # exception if the name is unknown
        identifier = self.get_dataset_identifier(name)
        # Read dataset from datastore and return it.
        dataset = self.datastore.get_dataset(identifier)
        if dataset is None:
            raise ValueError('unknown dataset \'' + identifier + '\'')
        return DatasetClient(dataset=dataset)

    def get_dataset_identifier(self, name):
        """Returns the unique identifier for the dataset with the given name.

        Raises ValueError if no dataset with the given name exists.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        string
        """
        # Datset names should be case insensitive
        key = name.lower()
        if not key in self.datasets:
            raise ValueError('unknown dataset \'' + name + '\'')
        return self.datasets[key]

    def has_dataset_identifier(self, name):
        """Test whether a mapping for the dataset with the given name exists.

        Parameters
        ----------
        name: string
            Dataset name

        Returns
        -------
        bool
        """
        # Dataset names are case insensitive
        return name.lower() in self.datasets

    def new_dataset(self):
        """Get a dataset client instance for a new dataset.

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        return DatasetClient()

    def remove_dataset_identifier(self, name):
        """Remove the entry in the dataset dictionary that is associated with
        the given name. Raises ValueError if not dataset with name exists.

        Parameters
        ----------
        name: string
            Dataset name
        identifier: string
            Unique identifier for persistent dataset
        """
        # Convert name to lower case to ensure that names are case insensitive
        key = name.lower()
        if not key in self.datasets:
            raise ValueError('unknown dataset \'' + name + '\'')
        del self.datasets[key]

    def rename_dataset(self, name, new_name):
        """Rename an existing dataset.

        Raises ValueError if a dataset with given name already exist.

        Raises ValueError if dataset with name does not exist or if dataset with
        new_name already exists.

        Parameters
        ----------
        name : string
            Unique dataset name
        new_name : string
            New dataset name
        """
        # Make sure to record access idependently of whether the dataset exists
        # or not. Ignore read access to datasets that have been written.
        if not name.lower() in self.write:
            self.read.add(name.lower())
        # Add the new name to the written datasets
        self.write.add(new_name.lower())
        # Raise exception if new_name exists or is not valid.
        if self.has_dataset_identifier(new_name):
            raise ValueError('dataset \'' + new_name + '\' exists')
        if not is_valid_name(new_name):
            raise ValueError('invalid dataset name \'' + new_name + '\'')
        # Raise an exception if no dataset with the given name exists
        identifier = self.get_dataset_identifier(name)
        self.drop_dataset(name)
        self.set_dataset_identifier(new_name, identifier)

    def set_dataset_identifier(self, name, identifier):
        """Sets the identifier to which the given dataset name points.

        Parameters
        ----------
        name: string
            Dataset name
        identifier: string
            Unique identifier for persistent dataset
        """
        # Convert name to lower case to ensure that names are case insensitive
        self.datasets[name.lower()] = identifier
        self.write.add(name.lower())

    def update_dataset(self, name, dataset):
        """Update a given dataset.

        Raises ValueError if the specified dataset does not exist.

        Parameters
        ----------
        name : string
            Unique dataset name
        dataset : vizier.datastore.base.Dataset
            Dataset object

        Returns
        -------
        vizier.datastore.client.DatasetClient
        """
        # Get identifier for the dataset with the given name. Will raise an
        # exception if the name is unknown
        identifier = self.get_dataset_identifier(name)
        # Read dataset from datastore to get the column and row counter.
        source_dataset = self.datastore.get_dataset(identifier)
        if source_dataset is None:
            # Record access to the datasets
            self.read.add(name.lower())
            raise ValueError('unknown dataset \'' + identifier + '\'')
        column_counter = source_dataset.max_column_id() + 1
        row_counter = source_dataset.max_row_id() + 1
        # Update column and row identifier
        columns = dataset.columns
        rows = dataset.rows
        # Ensure that all columns has positive identifier
        for col in columns:
            if col.identifier < 0:
                col.identifier = column_counter
                column_counter += 1
        # Ensure that all rows have positive identifier
        #for row in rows:
        #    if row.identifier < 0:
        #        row.identifier = row_counter
        #        row_counter += 1
        # Write dataset to datastore and add new dataset to context

        #gather up the read dependencies so that we can pass them to mimir
        # so that we can at least track coarse grained provenance.
        # TODO: we are asumming mimir dataset and datastore
        #       here and need to generalize this
        read_dep = []
        for dept_name in self.read:
            if not isinstance(dept_name, str):
                raise RuntimeError('invalid read name')
            dept_id = self.get_dataset_identifier(dept_name)
            dept_dataset = self.datastore.get_dataset(dept_id)
            read_dep.append(dept_dataset.table_name)
        ds = self.datastore.create_dataset(
            columns=columns,
            rows=rows,
            annotations=dataset.annotations,
            dependencies=read_dep
        )
        self.set_dataset_identifier(name, ds.identifier)
        self.descriptors[ds.identifier] = ds
        return DatasetClient(dataset=self.datastore.get_dataset(ds.identifier))

class Analyzer(ast.NodeVisitor):
    def __init__(self, name):
        self.name = name
        self.source = ''
        # track context name and set of names marked as `global`
        self.context = [('global', ())]

    def visit_FunctionDef(self, node):
        self.context.append(('function', set()))
        if node.name == self.name:
            self.source = "@vizierdb.export_module_decorator\n" + astor.to_source(node)
            self.generic_visit(node)
        self.context.pop()

    # treat coroutines the same way
    visit_AsyncFunctionDef = visit_FunctionDef

    def visit_Assign(self, node):
        self.context.append(('assignment', set()))
        target = node.targets[0]
        if target.id == self.name:
            self.source = "{} = vizierdb.wrap_variable({}, '{}')".format( self.name, astor.to_source(node.value), self.name)
            self.generic_visit(target)
        self.context.pop()

    def visit_ClassDef(self, node):
        self.context.append(('class', ()))
        if node.name == self.name:
            self.source = "@vizierdb.export_module_decorator\n" + astor.to_source(node)
            self.generic_visit(node)
        self.context.pop()

    def visit_Lambda(self, node):
        # lambdas are just functions, albeit with no statements, so no assignments
        self.context.append(('function', ()))
        self.generic_visit(node)
        self.context.pop()

    def visit_Global(self, node):
        assert self.context[-1][0] == 'function'
        self.context[-1][1].update(node.names)

    def visit_Name(self, node):
        ctx, g = self.context[-1]
        if node.id == self.name and (ctx == 'global' or node.id in g):
            print('exported {} at line {}'.format(node.id, node.lineno, self.source))

    def get_Source(self):
        return self.source
    
