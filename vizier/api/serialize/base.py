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

"""This module contains helper methods for the webservice that are used to
serialize web resources.
"""

from typing import List, Dict, Optional, Any

import vizier.api.serialize.labels as labels
from vizier.viztrail.module.output import ModuleOutputs, OutputObject


def HATEOAS(links: Dict[str, Optional[str]]) -> List[Dict[str, Optional[str]]]:
    """Convert a dictionary of key,value pairs into a list of references. Each
    list element is a dictionary that contains a 'rel' and 'href' element.

    Parameters
    ----------
    links: dict
        Dictionary where the key defines the relationship and the value is the
        url.

    Returns
    -------
    list
    """
    return [{labels.REL: key, labels.HREF: links[key]} for key in links]


def PROPERTIES(properties: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Serialize a dictionary of object properties into a list of dictionaries
    as expected by the web service API.

    Parameters
    ----------
    properties: dict

    Returns
    -------
    list
    """
    print("PROPS")
    print(properties)
    result = list()
    for key in properties:
        obj = {labels.KEY: key}
        value = properties[key]
        if not value is None:
            obj[labels.VALUE] = value
        result.append(obj)
    return result


# ------------------------------------------------------------------------------
# Output streams
# ------------------------------------------------------------------------------

def OUTPUT(out: OutputObject) -> Dict[str, Any]:
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


def OUTPUTS(output_streams: ModuleOutputs) -> Dict[str, Any]:
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
