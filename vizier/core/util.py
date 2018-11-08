# Copyright (C) 2018 New York University
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

"""Collection of helper methods."""

import uuid


"""Name of logger used for monitoring workflow engine performance."""
LOGGER_ENGINE = 'LOGGER_ENGINE'


# ------------------------------------------------------------------------------
# Helper Methods
# ------------------------------------------------------------------------------

def cast(value):
    """Attempt to convert a given value to integer or float. If both attempts
    fail the value is returned as is.

    Parameters
    ----------
    value: string

    Returns
    -------
    int, float, or string
    """
    # Return None if the value is None
    if value is None:
        return None
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def get_unique_identifier():
    """Create a new unique identifier.

    Returns
    -------
    string
    """
    return str(uuid.uuid4()).replace('-', '')


def is_valid_name(name):
    """Returns Ture if a given string represents a valid name (e.g., for a
    dataset). Valid names contain only letters, digits, hyphen, underline, or
    blanl. A valid name has to contain at least one digit or letter.

    Parameters
    ----------
    name : string
        Name for dataset in VizUAL dataset or column

    Returns
    -------
    bool
    """
    allnums = 0
    for c in name:
        if c.isalnum():
            allnums += 1
        elif not c in ['_', '-', ' ']:
            return False
    return (allnums > 0)


def min_max(values):
    """Return the min and the max value from a list of values.

    Parameters
    ----------
    values: list(scalar)
        List of values from a type for which '<' and '>' are defined.

    Returns
    -------
    scalar, scalar
    """
    if len(values) == 0:
        return None, None
    min_val = max_val = values[0]
    for i in range(1, len(values)):
        if min_val > values[i]:
            min_val = values[i]
        if max_val < values[i]:
            max_val = values[i]
    return min_val, max_val
