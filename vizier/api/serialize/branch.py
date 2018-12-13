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
serialize branches.
"""

import vizier.api.serialize.base as serialize


def BRANCH_DESCRIPTOR(branch, urls):
    """Dictionary serialization for branch descriptor.

    Parameters
    ----------
    branch : vizier.viztrail.branch.BranchHandle
        Branch handle
    urls: vizier.api.webservice.routes.UrlFactory
        Factory for resource urls

    Returns
    -------
    dict
    """
    branch_id = branch.identifier
    return {
        'id': branch_id,
        'properties': serialize.ANNOTATIONS(branch.properties),
        'links': serialize.HATEOAS({
            'self': urls.get_project(branch_id),
            'project:delete': urls.delete_project(branch_id),
            'project:update': urls.update_project(branch_id)
        })
    }
