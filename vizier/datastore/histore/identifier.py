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

"""Helper methods for dataset snapshot identifier. Since each dataset version
in Vizier is identified by a single string, for HISTORE we need to concatenate
the archive identifier and the snapshot version that contains the dataset."""


def create(archive_id, snapshot_id):
    """Get unique identifier for a dataset snapshot. The identifier is a
    concatenation of the dataset archive identifier and the snapshot version.

    Parameters
    ----------
    archive_id: string
        Unique dataset archive identifier.
    snapshot_id: int
        Unique snapshot version identifier.

    Returns
    -------
    string
    """
    return '{}-{}'.format(archive_id, snapshot_id)


def parse(identifier):
    """Parse a given dataset snapshot identifier. Returns a tuple containing
    the archive identifier and the snapshot version.

    Parameters
    ----------
    identifier: string
        Unique dataset snapshot identifier.

    Returns
    -------
    (string, int)

    Raises
    ------
    ValueError
    """
    pos = identifier.rfind('-')
    if pos == -1:
        raise ValueError("invalid identifier '{}'".format(identifier))
    return identifier[:pos], int(identifier[pos+1:])
