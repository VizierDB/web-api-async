# Copyright (C) 2017-2020 New York University,
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

"""Vizier DB - Database - Collection of objects and methods to maintain and
manipulate different versions of datasets that are manipulated by data curation
workflows.
"""

ARTIFACT_TYPE_DATASET = "application/dataset"
ARTIFACT_TYPE_PYTHON  = "application/python"
ARTIFACT_TYPE_TEXT    = "text/plain"

class ArtifactDescriptor(object):
    """The descriptor identifies an artifact read or written by a cell.  An Artifact may be an 
    opaque Object, or a transparent Dataset (in which case it should be identified by 
    vizier.datastore.dataset.DatasetDescriptor instead)

    Attributes
    ----------
    identifier: string
        Unique dataset identifier
    artifact_type: string
        The type of the artifact.  Typically either a MIME type, or "application/dataset"
    """
    def __init__(self, 
            identifier: str, 
            artifact_type: str
        ):
        """Initialize the dataset descriptor.

        Parameters
        ----------
        identifier: string
            Unique dataset identifier.
        artifact_type: string
            The type of the artifact.  Typically either a MIME type, or "application/dataset"
        """
        self.identifier = identifier
        self.artifact_type = artifact_type

    @property
    def is_dataset(self) -> bool:
      return self.artifact_type == ARTIFACT_TYPE_DATASET
