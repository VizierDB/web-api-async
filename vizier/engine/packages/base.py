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

"""Generic declarations for supported workflow commands. Commands that are
supported by Vizier are grouped into packages (e.g., VizUAL, Mimir Lenses,
Python, etc.). Each package defines a set of commands.

Package declarations define the structure of workflow commands that has to be
followed by requests that are made via the API. The declarations are also used
to define the front-end forms that are rendered in notebook cells to gather
user input.
"""

import json
import yaml
from jsonschema import validate, ValidationError


# ------------------------------------------------------------------------------
# Package Declaration
# ------------------------------------------------------------------------------

"""Components and schema of package declaration."""
LABEL_COMMAND = 'command'
LABEL_DATATYPE = 'datatype'
LABEL_DEFAULT = 'defaultValue'
LABEL_DELIMITER = 'delimiter'
LABEL_DESCRIPTION = 'description'
LABEL_FORMAT = 'format'
LABEL_GROUPID = 'groupId'
LABEL_ID = 'id'
LABEL_LANGUAGE = 'language'
LABEL_LEFTSPACE = 'lspace'
LABEL_NAME = 'name'
LABEL_PARAMETER = 'parameter'
LABEL_PARENT = 'parent'
LABEL_PREFIX = 'prefix'
LABEL_REQUIRED = 'required'
LABEL_RIGHTSPACE = 'rspace'
LABEL_SUFFIX = 'suffix'
LABEL_TYPE = 'type'
LABEL_VALUE = 'value'

PACKAGE_SCHEMA = {
    'type': 'object',
    'properties': {
        LABEL_ID: {'type': 'string'},
        LABEL_NAME: {'type': 'string'},
        LABEL_DESCRIPTION: {'type': 'string'},
        LABEL_COMMAND: {
            'type': 'array',
            'items': {
                'type': 'object',
                'properties': {
                    LABEL_ID: {'type': 'string'},
                    LABEL_NAME: {'type': 'string'},
                    LABEL_DESCRIPTION: {'type': 'string'},
                    LABEL_PARAMETER: {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                LABEL_ID: {'type': 'string'},
                                LABEL_NAME: {'type': 'string'},
                                LABEL_DESCRIPTION: {'type': 'string'},
                                LABEL_DATATYPE: {'type': 'string'},
                                LABEL_LANGUAGE: {'type': 'string'},
                                LABEL_PARENT: {'type': 'string'},
                                'values': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'object',
                                        'properties': {
                                            'isDefault': {'type': 'boolean'},
                                            'text': {'type': 'string'},
                                            'value': {'type': 'string'}
                                        },
                                        'required': ['value']
                                    }
                                },
                                LABEL_REQUIRED: {'type': 'boolean'},
                                LABEL_DEFAULT: {'oneOf': [
                                    {'type': 'boolean'},
                                    {'type': 'string'},
                                    {'type': 'number'}
                                ]},
                                'index': {'type': 'number'},
                                'hidden': {'type': 'boolean'}
                            },
                            'required': [LABEL_ID, LABEL_DATATYPE]
                        }
                    },
                    LABEL_FORMAT: {
                        'type': 'array',
                        'items': {
                            'type': 'object',
                            'properties': {
                                LABEL_TYPE: {'type': 'string'},
                                LABEL_VALUE: {'type': 'string'},
                                LABEL_LEFTSPACE: {'type': 'boolean'},
                                LABEL_RIGHTSPACE: {'type': 'boolean'},
                                LABEL_PREFIX: {'type': 'string'},
                                LABEL_SUFFIX: {'type': 'string'},
                                LABEL_FORMAT: {
                                    'type': 'array',
                                    'items': {
                                        'type': 'object',
                                        'properties': {
                                            LABEL_TYPE: {'type': 'string'},
                                            LABEL_VALUE: {'type': 'string'},
                                            LABEL_PREFIX: {'type': 'string'},
                                            LABEL_SUFFIX: {'type': 'string'}
                                        },
                                        'required': [LABEL_TYPE, LABEL_VALUE]
                                    }
                                }
                            },
                            'required': [LABEL_TYPE, LABEL_VALUE, LABEL_LEFTSPACE, LABEL_RIGHTSPACE]
                        }
                    }
                },
                'required': [LABEL_ID, LABEL_PARAMETER]
            }
        }
    },
    'required': [LABEL_ID, LABEL_COMMAND]
}

"""Definition of parameter data types."""
DT_BOOL = 'bool'
DT_CODE = 'code'
DT_COLUMN_ID = 'colid'
DT_DATASET_ID = 'dataset'
DT_DECIMAL = 'decimal'
DT_FILE_ID = 'fileid'
DT_INT = 'int'
DT_LIST = 'list'
DT_RECORD = 'record'
DT_ROW_INDEX = 'rowidx'
DT_ROW_ID = 'rowid'
DT_SCALAR = 'scalar'
DT_STRING = 'string'

DATA_TYPES = [
    DT_BOOL,
    DT_CODE,
    DT_COLUMN_ID,
    DT_DATASET_ID,
    DT_DECIMAL,
    DT_FILE_ID,
    DT_INT,
    DT_LIST,
    DT_RECORD,
    DT_ROW_INDEX,
    DT_ROW_ID,
    DT_SCALAR,
    DT_STRING
]

"""Argument data types that expect an integer or string values."""
INT_TYPES = [DT_COLUMN_ID, DT_INT, DT_ROW_INDEX, DT_ROW_ID]
STRING_TYPES = [DT_DATASET_ID, DT_STRING, DT_CODE]



"""Elements for fileid dictionaries."""
FILE_ID = 'fileid'
FILE_NAME = 'filename'
FILE_URL = 'url'
FILE_USERNAME = 'userName'
FILE_PASSWORD = 'password'
FILE_RELOAD = 'reload'


def package_declaration(identifier, commands, name=None, description=None):
    """Create a dictionary containing a package declaration.

    Parameters
    ----------
    identifier: string
        Unique package identifier
    commands: list
        List of package commands
    name: string, optional
        Optional package name
    description: string, optional
        Optional descriptive text for package

    Returns
    -------
    dict
    """
    obj = dict({LABEL_ID: identifier})
    obj[LABEL_COMMAND] = commands
    if not name is None:
        obj[LABEL_NAME] = name
    if not description is None:
        obj[LABEL_DESCRIPTION] = description
    return obj


def validate_package(pckg_declaration):
    """Validate a given package declaration. Includes validating declarations
    for package commands. Raises a ValueError if an invalid package
    declaration is given.

    Parameters
    ----------
    pckg_declaration: dict
        Dictionary containing package declaration

    """
    # Make sure that the given package declaration matches the schema
    try:
        validate(pckg_declaration, PACKAGE_SCHEMA)
    except ValidationError as ex:
        raise ValueError('failed to validate package declaration. ' + ex.message)
    # For each command parameter ensure that it has a valid datatype and that
    # the type of all format components is valid
    for cmd_declaration in pckg_declaration[LABEL_COMMAND]:
        for para_declaration in cmd_declaration[LABEL_PARAMETER]:
            dt = para_declaration[LABEL_DATATYPE]
            if not dt in DATA_TYPES:
                raise ValueError('invalid data type \'' + str(dt) + '\'')
        if LABEL_FORMAT in cmd_declaration:
            for el in cmd_declaration[LABEL_FORMAT]:
                if not el[LABEL_TYPE] in FORMAT_TYPE:
                    raise ValueError('invalid format element type \'' + str(el[LABEL_TYPE]) + '\'')


# ------------------------------------------------------------------------------
# Command Declaration
# ------------------------------------------------------------------------------

def command_declaration(identifier, name=None, description=None, group_id=None, parameters=None, format=None):
    """Create a dictionary containing a package command declaration.

    Parameters
    ----------
    identifier: string
        Unique command identifier
    name: string, optional
        Optional command name
    description: string, optional
        Optional descriptive text for command
    group_id: string, optional
        Optional group identifier to arrange commands on the front-end
    parameters: list, optional
        List of command parameter declarations
    format: list, optional
        Command format declaration to generate printable string representation
        for command instances

    Returns
    -------
    dict
    """
    obj = dict({LABEL_ID: identifier})
    if not name is None:
        obj[LABEL_NAME] = name
    else:
        obj[LABEL_NAME] = identifier
    if not description is None:
        obj[LABEL_DESCRIPTION] = description
    if not group_id is None:
        obj[LABEL_GROUPID] = description
    if not parameters is None:
        obj[LABEL_PARAMETER] = parameters
    else:
        obj[LABEL_PARAMETER] = list()
    if not format is None:
        obj[LABEL_FORMAT] = format
    return obj


# ------------------------------------------------------------------------------
# Parameter Declaration
# ------------------------------------------------------------------------------

"""Definition of common module parameter names."""
PARA_COLUMN = 'column'
PARA_DATASET = 'dataset'
PARA_NAME = 'name'


def enum_value(value, text=None, is_default=False):
    """Create dictionary representing a value in an enumeration of possible
    values for a command parameter.

    Parameters
    ----------
    value: scalar
        Enumeration value
    text: string, optional
        Text representation for the value in the front end
    is_default: bool, optional
        Flag indicating whether this is the default value for the list

    Returns
    -------
    dict
    """
    obj = {'value': value, 'isDefault': is_default}
    if not text is None:
        obj['text'] = text
    else:
        obj['text'] = str(value)
    return obj


def parameter_declaration(
        identifier, name=None, data_type=None, index=0, required=True,
        values=None, parent=None, language=None, default_value=None, hidden=False
    ):
    """Create a dictionary that contains a module parameter specification.

    Parameters
    ----------
    identifier: string
        Unique parameter identifier
    name: string
        Printable parameter name
    data_type: string
        Parameter type
    index: int
        Index position of argument in input form
    required: bool, optional
        Required flag
    values: list, optional
        List of valid parameter values
    parent: int, optional
        Identifier of a grouping element
    language: string, optional
        If the parameter is of type 'code' the language can be used to further
        specify the programming language / grammar for the code text.
    default_value: bool, string, number, optional
        Optional default value for a scalar parameter
    hidden: bool, optional
        Defines if the parameter is vizible or hidden when displaying an input
        for at the front-end

    Returns
    -------
    dict
    """
    if not data_type in DATA_TYPES:
        raise ValueError('invalid parameter data type \'' + data_type + '\'')
    para = {
        LABEL_ID: identifier,
        LABEL_NAME: name,
        LABEL_DATATYPE: data_type,
        'index': index,
        'required': required,
        'hidden': hidden
    }
    if not values is None:
        para['values'] = values
    if not parent is None:
        para[LABEL_PARENT] = parent
    if not language is None:
        para[LABEL_LANGUAGE] = language
    if not default_value is None:
        para[LABEL_DEFAULT] = default_value
    return para


def para_column(index, parent=None):
    """Return dictionary specifying the default column parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_declaration(
        PARA_COLUMN,
        name='Column',
        data_type=DT_COLUMN_ID,
        index=index,
        parent=parent
    )


def para_dataset(index):
    """Return dictionary specifying the default dataset parameter used by most
    modules.

    Returns
    -------
    dict
    """
    return parameter_declaration(
        PARA_DATASET,
        name='Dataset',
        data_type=DT_DATASET_ID,
        index=index
    )


# ------------------------------------------------------------------------------
# Format Declaration
# ------------------------------------------------------------------------------

"""Format element types"""

FORMAT_CONST = 'const'
FORMAT_OPTION = 'opt'
FORMAT_VARIABLE = 'var'

FORMAT_TYPE = [FORMAT_CONST, FORMAT_OPTION, FORMAT_VARIABLE]


def format_element(
    type, value, nested_format=None, prefix=None, suffix=None, delimiter=None,
    lspace=True, rspace=True
):
    """Generic dictionary for elements in a command format declaration.

    Parameters
    ----------
    type: string
        Unique element type identifier
    value: string
        Type-specific element value
    nested_format: list, optional
        Optional format for list elements (only if element type is 'var' and
        the data type of the referenced parameter is 'list').
    prefix: string, optional
        Optional prefix for variable format elements that are only output if the
        parameter value is present.
    suffix: string, optional
        Optional suffix for variable format elements that are only output if the
        parameter value is present.
    delimiter: string, optional
        Optional delimiter for elements in a nested listing
    lspace: bool, oprional
        Space to left of constant if True
    rspace: bool, optional
        Space to right of constant if true

    Returns
    -------
    dict
    """
    obj = dict({
        LABEL_TYPE: type,
        LABEL_VALUE: value,
        LABEL_LEFTSPACE: lspace,
        LABEL_RIGHTSPACE: rspace
    })
    if not nested_format is None:
        obj[LABEL_FORMAT] = nested_format
    if not prefix is None:
        obj[LABEL_PREFIX] = prefix
    if not suffix is None:
        obj[LABEL_SUFFIX] = suffix
    if not delimiter is None:
        obj[LABEL_DELIMITER] = delimiter
    return obj


def constant_format(value, lspace=True, rspace=True):
    """Dictionary for a constant in a format declaration for a package command.

    Parameters
    ----------
    value: string
        Constant output value
    lspace: bool, oprional
        Space to left of constant if True
    rspace: bool, optional
        Space to right of constant if true

    Returns
    -------
    dict
    """
    return format_element(
        type=FORMAT_CONST,
        value=value,
        lspace=lspace,
        rspace=rspace
    )


def group_format(parameter_id, format, delimiter=None):
    """Dictionary for a variable value in a format declaration for a package
    command that references a list of values. Creates a nested format element.

    Parameters
    ----------
    parameter_id: string
        Identifier of the referenced parameter
    format: list
        Format for list elements
    delimiter: string, optional
        Optional delimiter for concatenated list elements

    Returns
    -------
    dict
    """
    return format_element(
        type=FORMAT_VARIABLE,
        value=parameter_id,
        nested_format=format,
        delimiter=delimiter
    )


def optional_format(parameter_id, prefix=None, suffix=None):
    """Dictionary for a variable value in a format declaration for a package
    command that references a parameter and is dependent on the parameter being
    present.

    Parameters
    ----------
    parameter_id: string
        Identifier of the referenced parameter
    prefix: string, optional
        Optional prefix for variable format elements that are only output if the
        parameter value is present.
    suffix: string, optional
        Optional suffix for variable format elements that are only output if the
        parameter value is present.

    Returns
    -------
    dict
    """
    return format_element(
        type=FORMAT_OPTION,
        value=parameter_id,
        prefix=prefix,
        suffix=suffix
    )

def variable_format(parameter_id):
    """Dictionary for a variable value in a format declaration for a package
    command.

    Parameters
    ----------
    parameter_id: string
        Identifier of the referenced parameter

    Returns
    -------
    dict
    """
    return format_element(type=FORMAT_VARIABLE, value=parameter_id)


# ------------------------------------------------------------------------------
# Package Index
# ------------------------------------------------------------------------------

class PackageIndex(object):
    """Index of package command declarations. Maintains a dictionary that can be
    used to instantiate objects of the associated package task engine.
    """
    def __init__(self, package):
        """Initialize the index from a package declaration.

        Validates the given package declaration. Raises ValueError if an
        invalid package declaration is given.

        Parameters
        ----------
        package: dict
            Package declaration
        """
        # Validate the given package specification
        validate_package(package)
        # Keep a copy of the package identifier, name and descripton.
        self.identifier = package[LABEL_ID]
        if LABEL_NAME in package:
            self.name = package[LABEL_NAME]
        else:
            self.name = self.identifier
        if LABEL_DESCRIPTION in package:
            self.description = package[LABEL_DESCRIPTION]
        else:
            self.description = None
        # Create a class loader for the package engine
        self.commands = dict()
        for cmd in package[LABEL_COMMAND]:
            self.commands[cmd[LABEL_ID]] = CommandDeclaration(
                identifier=cmd[LABEL_ID],
                name=cmd[LABEL_NAME],
                description=cmd[LABEL_DESCRIPTION] if LABEL_DESCRIPTION in cmd else None,
                paramaters=cmd[LABEL_PARAMETER],
                format=cmd[LABEL_FORMAT] if LABEL_FORMAT in cmd else list()
            )

    def get(self, command_id):
        """Get the parameter declarations for the given command.

        Raises ValueError if no command with the given identifier exists.

        Parameters
        ----------
        command_id: string
            Unique command identifier

        Returns
        -------
        vizier.workflow.packages.base.CommandDeclaration
        """
        if not command_id in self.commands:
            raise ValueError('unknown command \'' + str(command_id) + '\'')
        return self.commands[command_id]


class CommandDeclaration(object):
    """Command declaration contains an index of command parameter declarations
    and the command format declaration.
    """
    def __init__(self, identifier, name, paramaters, format, description=None):
        """Initialize the index from a given list of paramater declarations.

        Parameters
        ----------
        identifier: string
            Unique command identifier
        name: string
            Descriptive command name
        parameters: list
            List of parameter declarations
        format: list
            List of format specifications for tokens in the external form of the
            command
        description: string, optional
            Optional description for the command
        """
        self.identifier = identifier
        self.name = name
        self.description = description
        self.parameters = dict()
        for para in paramaters:
            self.parameters[para[LABEL_ID]] = para
        self.format = format

    def get(self, parameter_id):
        """Get declaration for parameter with given identifier.

        Raises ValueError if no parameter with the given identifier exists.

        Parameters
        ----------
        parameter_id: string
            Unique parameter identifier

        Returns
        -------
        dict
        """
        if not parameter_id in self.parameters:
            raise ValueError('unknown parameter \'' + str(parameter_id) + '\'')
        return self.parameters[parameter_id]

    def mandatory(self, parent=None):
        """Get a list of parameter names that are mandatory. The optional parent
        parameter allows to request mandatory parameter for nested components.

        Parameters
        ----------
        parent: string, optional
            Optional name of parameter parent

        Returns
        -------
        list(string)
        """
        result = list()
        for para in self.parameters.values():
            if not para[LABEL_REQUIRED]:
                continue
            if not parent is None and LABEL_PARENT in para:
                if para[LABEL_PARENT] == parent:
                    result.append(para[LABEL_ID])
            elif parent is None and not LABEL_PARENT in para:
                result.append(para[LABEL_ID])
        return result


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def export_package(filename, package_key, package_def, format='YAML'):
    """Write the package definition to to the given file in Yaml format.

    Parameters
    ----------
    filename: string
        Name of the output file.
    package_key: string
        Unique package identifier
    package_def: dict
        Definition of commands that are supported by the package
    """
    if not format.upper() in ['JSON', 'YAML']:
        raise ValueError('invalid output format \'' + str(format) + '\'')
    with open(filename, 'w') as f:
        if format.upper() == 'YAML':
            yaml.dump({package_key: package_def}, f, default_flow_style=False)
        else:
            json.dump({package_key: package_def}, f, sort_keys=True, indent=4)
