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

"""This module contains helper methods for the webservice that are used to
serialize web resources.
"""

from vizier.viztrail.module.output import ModuleOutputs, OutputObject


def ANNOTATIONS(annotations):
    """Serialize the content of an object annotation set.

    Parameters
    ----------
    annotations: vizier.core.annotation.base.ObjectAnnotationSet
        Set of object annotations

    Returns
    -------
    dict()
    """
    values = annotations.values()
    return [{'key': key, 'value': values[key]} for key in values]


def HATEOAS(links):
    """Convert a dictionary of key,value pairs into a list of references. Each
    list element is a dictionary that contains a 'rel' and 'href' element.

    Parameters
    ----------
    links: dict()
        Dictionary where the key defines the relationship and the value is the
        url.

    Returns
    -------
    list()
    """
    return [{'rel': key, 'href': links[key]} for key in links]


# ------------------------------------------------------------------------------
# Output streams
# ------------------------------------------------------------------------------

def OUTPUT(out):
    """Get dictionary serialization for an output object.

    Parameters
    ----------
    out: vizier.viztrail.module.output.OutputObject
        Object in module output stream

    Returns
    -------
    dict
    """
    return {'type': out.type, 'value': out.value}


def OUTPUTS(output_streams):
    """Get dictionary serialization for a pair of STDOUT and STDERR output
    stream.

    Parameters
    ----------
    output_streams: vizier.viztrail.module.output.ModuleOutputs
        Module output streams

    Returns
    -------
    dict()
    """
    return {
        'stdout': [OUTPUT(out) for out in output_streams.stdout],
        'stderr': [OUTPUT(out) for out in output_streams.stderr]
    }
