# Copyright (C) 2018 New York University,
#                    University at Buffalo,
#                    Illinois Institute of Technology.
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

"""Workflow commands are defined by a triple of package identifier, command
identifier and command arguments. The command identifier specifies a command
within the identified package.

Command arguments are expected to be a list of dictionaries. Each list element
specifies the value for one command parameter. The dictionaries are expected to
contain the parameter id and value as dictionary elements. The structure of the
va;ue element is dependent on the parameter type.
"""

import vizier.workflow.packages.base as pckg


"""Element labels for dictionaries that represent command arguments."""
ARG_ID = 'id'
ARG_VALUE = 'value'


class ModuleArguments(object):
    """Nested dictionary of module command arguments."""
    def __init__(self, arguments=[], parent=None):
        """Initialize the arguments from a given list of dictionaries that are
        (id,value)-pairs. Values can be lists in which case a nested argument
        structure is assumed.

        Parameters
        ----------
        arguments: list, optional
            List of dictionaries that define arguments for command parameters
        parent: string, optional
            Optional parent name for nested arguments
        """
        self.arguments = dict()
        self.parent = parent
        for arg in arguments:
            identifier = arg[ARG_ID]
            value = arg[ARG_VALUE]
            if isinstance(value, list):
                # The list elements are either all dictionaries or all lists.
                has_lists = False
                has_records = False
                for el in value:
                    if isinstance(el, list):
                        has_lists = True
                    elif isinstance(el, dict):
                        has_records = True
                    else:
                        raise ValueError('invalid argument specification')
                    if has_lists and has_records:
                        raise ValueError('invalid argument specification')
                if has_records:
                    value = ModuleArguments(value, parent=identifier)
                else:
                    elements = list()
                    for el in value:
                        elements.append(ModuleArguments(el, parent=identifier))
                    value = elements
            self.arguments[identifier] = value

    def get(self, name):
        """Get the value for the parameter with the given name.

        Parameters
        ----------
        name: string
            Parameter name
        Returns
        -------
        any
        """
        return self.arguments[name]

    def has(self, name):
        """Test if a value for the parameter with the given name has been
        provided.

        Parameters
        ----------
        name: string
            Parameter name

        Returns
        -------
        bool
        """
        return name in self.arguments

    def to_list(self):
        """Get list serialization of the arguments listing.

        Returns
        -------
        dict
        """
        result = list()
        for arg_id in self.arguments:
            arg_val = self.arguments[arg_id]
            if isinstance(arg_val, ModuleArguments):
                arg_val = arg_val.to_list()
            elif isinstance(arg_val, list):
                elements = list()
                for el in arg_val:
                    elements.append(el.to_list())
                arg_val = elements
            result.append({ARG_ID: arg_id, ARG_VALUE: arg_val})
        return result

    def validate(self, parameters):
        """Validate the module arguments against the given command parameter
        declarations.

        Parameters
        -----------
        parameters: vizier.workflow.packages.base.ParameterIndex
        """
        # Keep track of the parameter identifier in the argument list
        keys = set()
        for arg_id in self.arguments:
            arg_value = self.arguments[arg_id]
            # Get the parameter declaration. This will raise an exception if the
            # parameter id is unknown.
            para = parameters.get(arg_id)
            datatype = para[pckg.LABEL_DATATYPE]
            if datatype == pckg.DT_BOOL:
                if not isinstance(arg_value, bool):
                    raise ValueError('expected bool for \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_DECIMAL:
                if not isinstance(arg_value, float):
                    raise ValueError('expected float for \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_SCALAR:
                if not isinstance(arg_value, float) and not isinstance(arg_value, int):
                    raise ValueError('expected scalar for \'' + str(arg_id) + '\'')
            elif datatype in pckg.INT_TYPES:
                if not isinstance(arg_value, int):
                    raise ValueError('expected int for \'' + str(arg_id) + '\'')
            elif datatype in pckg.STRING_TYPES:
                if not isinstance(arg_value, basestring):
                    raise ValueError('expected string for \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_LIST:
                if not isinstance(arg_value, list):
                    raise ValueError('expected list for \'' + str(arg_id) + '\'')
                for args in arg_value:
                    args.validate(parameters)
            elif datatype == pckg.DT_RECORD:
                if not isinstance(arg_value, ModuleArguments):
                    raise ValueError('expected list for \'' + str(arg_id) + '\'')
                arg_value.validate(parameters)
            else:
                raise RuntimeError('unknonw data type \'' + str(datatype) + '\'')
            keys.add(arg_id)
        # Ensure that all mandatory arguments are given
        for key in parameters.mandatory(parent=self.parent):
            if not key in keys:
                raise ValueError('missing value for parameter \'' + key + '\'')


class ModuleCommand(object):
    """Specification of a command that is executed by a workflow module. The
    command specification includes identifier for the package and the command
    itself. The specification also includes a list of arguments, one for each of
    the (mandatory) command parameters.

    Attributes
    ----------
    package_id: string
        Unique package identifier
    command_id: string
        Package-specific unique command identifier
    arguments: vizier.workflow.module.command.ModuleArguments
        Nested structure of arguments for the specified command
    """
    def __init__(self, package_id, command_id, arguments=[], packages=None):
        """Initialize the package and command identifier as well as the command
        arguments.

        If the package repository argument is provided the command specification
        is validated. A ValueError is raised if the specified command is
        invalid.

        Parameters
        ----------
        package_id: string
            Unique package identifier
        command_id: string
            Package-specific unique command identifier
        arguments: list, optional
            List of dictionaries that define arguments for command parameters
        packages: dict(vizier.workflow.package.base.PackageIndex)
            Dictionary of packages
        """
        self.package_id = package_id
        self.command_id = command_id
        self.arguments = ModuleArguments(arguments=arguments)
        # Validate the command if the package repository is given
        if not packages is None:
            if not package_id in packages:
                raise ValueError('unknown package \'' + str(package_id) + '\'')
            # The package index .get() method will raise an exception if the
            # given command is unknown. The .validate() method will raise
            # an exception if any of the given arguments is invalid or if a
            # mandatory argument is missing.
            self.arguments.validate(packages[package_id].get(command_id))


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def ARG(id, value):
    """Get dictionary for simple module argument.

    Parameters
    ----------
    id: string
        Argument identifier
    value: any
        Argument value

    Returns
    -------
    dict
    """
    return {ARG_ID: id, ARG_VALUE: value}
