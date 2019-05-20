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

"""Workflow commands are defined by a triple of package identifier, command
identifier and command arguments. The command identifier specifies a command
within the identified package.

Command arguments are expected to be a list of dictionaries. Each list element
specifies the value for one command parameter. The dictionaries are expected to
contain the parameter id and value as dictionary elements. The structure of the
va;ue element is dependent on the parameter type.
"""

from vizier.core.util import is_scalar

import vizier.engine.packages.base as pckg


"""Element labels for dictionaries that represent command arguments."""
ARG_ID = 'id'
ARG_VALUE = 'value'

"""Constant to represent unknonw package or command identifier in an error
state.
"""
UNKNOWN_ID = 'unknown'


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

    def get_dataset(self, command):
        """Return the value of the first arument of type DATASET_ID or None.

        Parameters
        ----------
        command: vizier.engine.packages.base.CommandDeclaration
            Command declaration containing parameters and format declaration

        Returns
        -------
        string
        """
        for para in command.parameters.values():
            if para[pckg.LABEL_DATATYPE] == pckg.DT_DATASET_ID:
                arg = para[pckg.LABEL_ID]
                if self.has(arg):
                    return self.get(arg)
                else:
                    return None
        return None

    def get_value(self, key, as_int=False, raise_error=True, default_value=None):
        """Retrieve command argument with given key. Will raise ValueError if no
        argument with given key is present.

        Parameters
        ----------
        key : string
            Argument name
        as_int : bool
            Flag indicating whether the argument should be converted to integer.
            If the argument value cannot be converted to integer the value is
            returned as is.
        raise_error: bool, optional
            Flag indicating whether to raise a ValueError if the object does not
            contain an argument for the given key. If False, the default_value
            will be returned instead. This flag is always  if the default value
            is not None.
        default_value: string, optional
            Default value that is returned if no argument for the given key
            exists

        Returns
        -------
        dict
        """
        if not key in self.arguments:
            if raise_error and default_value is None:
                raise ValueError('missing argument \'' + key + '\'')
            else:
                return default_value
        val = self.arguments[key]
        if as_int:
            try:
                val = int(val)
            except ValueError as ex:
                pass
        return val

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

    def to_external_form(self, command, datasets=None, format=None, parent_ds_name=None):
        """Get a string representation for the command based on the internal
        values for the module arguments.
        values.

        Parameters
        ----------
        command: vizier.engine.packages.base.CommandDeclaration
            Command declaration containing parameters and format declaration
        datasets: dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Datasets in the current database state keyed by the dataset name

        Returns
        -------
        string
        """
        # Set the dataset name if the command has a dataset parameter
        ds_name = self.get_dataset(command)
        if ds_name is None:
            ds_name = parent_ds_name
        # Set the format specification if not given
        if format is None:
            format = command.format
        tokens = list()
        for element in format:
            token = None
            var = None
            if element[pckg.LABEL_TYPE] == pckg.FORMAT_CONST:
                token = element[pckg.LABEL_VALUE]
            else:
                try:
                    # Get variable declaration
                    var = command.get(element[pckg.LABEL_VALUE])
                    arg = var[pckg.LABEL_ID]
                    if self.has(arg):
                        value = self.get(arg)
                        if var[pckg.LABEL_DATATYPE] == pckg.DT_COLUMN_ID:
                            token = get_column_name(
                                ds_name=ds_name.lower(),
                                column_id=value,
                                datasets=datasets
                            )
                        elif var[pckg.LABEL_DATATYPE] == pckg.DT_FILE_ID:
                            if pckg.FILE_URL in value:
                                token = value[pckg.FILE_URL]
                            elif pckg.FILE_NAME in value:
                                token = format_str(value[pckg.FILE_NAME])
                            elif pckg.FILE_ID in value:
                                token = format_str(value[pckg.FILE_ID])
                            else:
                                token = '?file?'
                        elif var[pckg.LABEL_DATATYPE] == pckg.DT_LIST:
                            if pckg.LABEL_FORMAT in element:
                                subcmd = list()
                                for val in value:
                                    subtoken = val.to_external_form(
                                        command=command,
                                        format=element[pckg.LABEL_FORMAT],
                                        datasets=datasets,
                                        parent_ds_name=ds_name
                                    )
                                    if not subtoken is None:
                                        subcmd.append(subtoken)
                                if len(subcmd) > 0:
                                    if pckg.LABEL_DELIMITER in element:
                                        delimiter = element[pckg.LABEL_DELIMITER]
                                    else:
                                        delimiter = ', '
                                    token = delimiter.join(subcmd)
                        elif var[pckg.LABEL_DATATYPE] != pckg.DT_CODE:
                            token = format_str(str(value))
                        else:
                            token = str(value)
                        if pckg.LABEL_PREFIX in element:
                            token = element[pckg.LABEL_PREFIX] + token
                        if pckg.LABEL_SUFFIX in element:
                            token = token + element[pckg.LABEL_SUFFIX]
                    elif element[pckg.LABEL_TYPE] == pckg.FORMAT_VARIABLE:
                        token = arg
                except ValueError as ex:
                    token = '?' + element[pckg.LABEL_VALUE] + '?'
            if not token is None:
                append_token(
                    tokens,
                    token,
                    lspace=element[pckg.LABEL_LEFTSPACE],
                    rspace=element[pckg.LABEL_RIGHTSPACE]
                )
        return concat_tokens(tokens)

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
        parameters: vizier.engine.packages.base.CommandDeclaration
        """
        # Keep track of the parameter identifier in the argument list
        keys = set()
        required_key_values = parameters.mandatory(parent=self.parent)
        
        for arg_id in self.arguments:
            arg_value = self.arguments[arg_id]
            # Get the parameter declaration. This will raise an exception if the
            # parameter id is unknown.
            para = parameters.get(arg_id)
            datatype = para[pckg.LABEL_DATATYPE]
            if arg_value is None:
                if arg_id in required_key_values:
                    raise ValueError('missing value for parameter \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_BOOL:
                if not isinstance(arg_value, bool):
                    raise ValueError('expected bool for \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_DECIMAL:
                if not isinstance(arg_value, float):
                    raise ValueError('expected float for \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_SCALAR:
                if not is_scalar(arg_value):
                    raise ValueError('expected scalar for \'' + str(arg_id) + '\'')
            elif datatype in pckg.INT_TYPES:
                if not isinstance(arg_value, int):
                    raise ValueError('expected int for \'' + str(arg_id) + '\'')
            elif datatype in pckg.STRING_TYPES:
                if not isinstance(arg_value, basestring):
                    raise ValueError('expected string for \'' + str(arg_id) + '\'')
            elif datatype == pckg.DT_FILE_ID:
                # Expects a dictinary that contains at least a fileId or the
                # an uri element
                if not isinstance(arg_value, dict):
                    raise ValueError('expected dictionary for \'' + str(arg_id) + '\'')
                if not pckg.FILE_ID in arg_value and not pckg.FILE_URL in arg_value:
                    raise ValueError('invalid file identifier')
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
        for key in required_key_values:
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
    arguments: vizier.viztrail.command.ModuleArguments
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
        packages: dict(vizier.engine.package.base.PackageIndex)
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

    @staticmethod
    def from_dict(doc):
        """Create module command instance from dictionary serialization.

        Parameters
        ----------
        doc: dict
            Dictionary serialization as created by .to_dict() method.

        Returns
        -------
        vizier.viztrail.command.ModuleCommand
        """
        return ModuleCommand(
            package_id=doc['package'],
            command_id=doc['command'],
            arguments=doc['arguments']
        )

    def to_dict(self):
        """Get a dictionary representation of the command specification.

        Returns
        -------
        dict()
        """
        return {
            'package': self.package_id,
            'command': self.command_id,
            'arguments': self.arguments.to_list()
        }

    def to_external_form(self, command, datasets=None):
        """Get a string representation for the command based on the current
        arguments.

        Parameters
        ----------
        command: vizier.engine.packages.base.CommandDeclaration
            Command declaration containing parameters and format declaration
        datasets: dict(vizier.datastore.dataset.DatasetDescriptor), optional
            Datasets in the current database state keyed by the dataset name

        Returns
        -------
        string
        """
        return self.arguments.to_external_form(
            command=command,
            datasets=datasets
        )


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


def append_token(tokens, token, lspace=True, rspace=True):
    """Append the token to the given output token list. Tokens are separated by
    boolean values that indicate whether there is a space between consecutive
    tokens or not.

    Parameters
    ----------
    tokens: list
        List of string and boolean values
    token: string
        Next token in output list
    lspace: bool, optional
        Left delimiter is space if True
    rspace: bool, optional
        Right delimiter is space if True
    """
    if len(tokens) > 0:
        tokens[-1] = tokens[-1] and lspace
    tokens.append(token)
    tokens.append(rspace)


def concat_tokens(tokens):
    """Concatenate the tokens in the output list.

    Parameters
    ----------
    tokens: list
        List of string and boolean values

    Returns
    -------
    string
    """
    if len(tokens) > 0:
        result = tokens[0]
        index = 1
        while index < len(tokens) - 1:
            if tokens[index]:
                result += ' '
            index += 1
            result += tokens[index]
            index += 1
        return result
    else:
        return ''

def format_str(value, default_value='?'):
    """Format an output string. Simply puts single quotes around the value if
    it contains a space character. If the given argument is none the default
    value is returned

    Parameters
    ----------
    value: string
        Given input value. Expected to be of type string
    default_value: string
        Default value that is returned in case value is None

    Returns
    -------
    string
    """
    if value is None:
        return default_value
    if ' ' in str(value):
        return '\'' + str(value) + '\''
    else:
        return str(value)


def get_column_name(ds_name, column_id, datasets):
    """Get the name of a column with given id in the dataset with ds_name from
    the current context. Returns col_id if the respective column is not found.

    Parameters
    ----------
    ds_name: string or None
        Name of the dataset (or None if argument was missing)
    column_id: str or int or None
        Identifier of dataset column (may be missing)
    datasets: dict(vizier.datastore.dataset.DatasetDescriptor), optional
        Datasets in the current database state keyed by the dataset name

    Returns
    -------
    string
    """
    if not datasets is None:
        try:
            # Get descriptor for dataset with given name
            if ds_name in datasets:
                col = datasets[ds_name].column_by_id(int(column_id))
                if not col is None:
                    return format_str(col.name)
        except Exception:
            pass
    return 'ID=' + str(column_id)
