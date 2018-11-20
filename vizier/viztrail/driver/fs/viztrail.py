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

"""Implements the viztrail handle for the file system based repository."""

import os
import shutil

from vizier.core.annotation.fs import PersistentAnnotationSet
from vizier.core.util import get_unique_identifier
from vizier.core.timestamp import get_current_time, to_datetime
from vizier.viztrail.driver.fs.io import DefaultObjectStore
from vizier.viztrail.driver.fs.branch import FSBranchHandle
from vizier.viztrail.base import ViztrailHandle


class FSViztrailHandle(ViztrailHandle):
    """Handle for viztrail in a file system based repository. All viztrail
    resources are maintained in several subfolders and files under the viztrail
    base directory.

    Files and Directories
    ---------------------
    /properties.json : Viztrail annotations
    /viztrail.json   : Viztrail metadata (identifier, timestamp, environment)
    /branches/       : Viztrail branches
    /modules/        : Modules in viztrail workflows
    /workflows/      : Viztrail workflow versions
    """
    def __init__(
        self, identifier, exec_env_id, properties, base_dir, object_store=None,
        branches=None, created_at=None
    ):
        """Initialize the viztrail descriptor.

        Parameters
        ----------
        identifier : string
            Unique viztrail identifier
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        properties: vizier.core.annotation.base.ObjectAnnotationSet
            Handler for user-defined properties
        base_dir : string
            Path to base directory for viztrail resources
        branches: list(vizier.viztrail.branch.BranchHandle)
            List of branches in the viztrail
        created_at : datetime.datetime, optional
            Timestamp of project creation (UTC)
        object_store: vizier.viztrail.driver.fs.io.ObjectStore, optional
            Object store implementation to read and write Json files
        """
        super(FSViztrailHandle, self).__init__(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=properties,
            branches=branches,
            created_at=created_at,
            last_modified_at=created_at
        )
        self.base_dir = base_dir
        self.object_store = object_store if not object_store is None else DefaultObjectStore()

    def create_branch(self, provenance=None, properties=None, workflow=None):
        """Create a new branch. If the workflow is given the new branch contains
        exactly this workflow. Otherwise, the branch is empty.

        Parameters
        ----------
        provenance: vizier.viztrail.base.BranchProvenance
            Provenance information for the new branch
        properties: dict, optional
            Set of properties for the new branch
        workflow: vizier.viztrail.workflow.WorkflowHandle, optional
            Head of branch

        Returns
        -------
        vizier.viztrail.branch.BranchHandle
        """
        # Get unique identifier for new branch. Raise runtime error if the
        # returned identifier is not unique.
        identifier = get_unique_identifier()
        if identifier in self.branches:
            raise RuntimeError('non-unique identifier \'' + str(identifier) + '\'')
        branch_dir = os.path.join(DIR_BRANCHES(self.base_dir), identifier)
        # Create materialized branch resource
        branch = FSBranchHandle.create_branch(
            identifier=identifier,
            provenance=provenance,
            properties=properties,
            workflow=workflow,
            base_dir=branch_dir,
            object_store=self.object_store
        )
        # Add the new resource to the branch index and return the branch handle
        self.branches[identifier] = branch
        return branch

    @staticmethod
    def create_viztrail(identifier, exec_env_id, base_dir, object_store=None, properties=None) :
        """Create a new viztrail resource. Will create the base directory for
        the viztrail. If the directory exists a ValueError is raised.

        Creates subfolders for viztrail resources. Writes viztrail metadata and
        properties to file.

        Parameters
        ----------
        properties: dict
            Set of properties for the new viztrail
        exec_env_id: string
            Identifier of the execution environment that is used for the
            viztrail
        base_dir: string
            Path to viztrail base directory
        object_store: vizier.viztrail.driver.fs.io.ObjectStore, optional
            Object store implementation to read and write Json files

        Returns
        -------
        vizier.viztrail.driver.fs.viztrail.FSViztrailHandle
        """
        # Create the base directory. Raises exception if the path refers to an
        # existing file or directory
        if os.path.exists(base_dir):
            raise ValueError('viztrail base directory \'' + str(base_dir) + '\' exists')
        os.makedirs(base_dir)
        # Create subfolders for branches, workflows, and modules
        os.makedirs(DIR_BRANCHES(base_dir))
        os.makedirs(DIR_MODULES(base_dir))
        os.makedirs(DIR_WORKFLOWS(base_dir))
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Write viztrail metadata to disk
        created_at = get_current_time()
        object_store.write(
            filename=FILE_VIZTRAIL(base_dir),
            content={
                'id': identifier,
                'env': exec_env_id,
                'createdAt': created_at.isoformat()
            }
        )
        # Return handle for new viztrail
        return FSViztrailHandle(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=PersistentAnnotationSet(
                filename=FILE_PROPERTIES(base_dir),
                annotations=properties
            ),
            created_at=created_at,
            base_dir=base_dir,
            object_store=object_store
        )

    def delete_viztrail(self) :
        """Deletes the directory that contains all resoures that are associated
        with this viztrail.
        """
        shutil.rmtree(self.base_dir)

    def delete_branch(self, branch_id):
        """Delete branch with the given identifier. Returns True if the branch
        existed and False otherwise.

        Parameters
        ----------
        branch_id: string
            Unique branch identifier

        Returns
        -------
        bool
        """
        if branch_id in self.branches:
            self.branches[branch_id].delete_branch()
            del self.branches[branch_id]
            return True
        else:
            return False

    @staticmethod
    def load_viztrail(base_dir, object_store=None):
        """Load all viztrail resources from files in the base directory.

        Parameters
        ----------
        base_dir: string
            Path to viztrail base directory
        object_store: vizier.viztrail.driver.fs.io.ObjectStore, optional
            Object store implementation to read and write Json files
        Returns
        -------
        vizier.viztrail.driver.fs.viztrail.FSViztrailHandle
        """
        # Make sure the object store is not None
        if object_store is None:
            object_store = DefaultObjectStore()
        # Load viztrail metadata
        metadata = object_store.read(FILE_VIZTRAIL(base_dir))
        identifier = metadata['id']
        created_at = to_datetime(metadata['createdAt'])
        exec_env_id = metadata['env']
        # Load existing branches. Every subfolder in the branches base directory
        # is expected to contain a branch
        branches = list()
        for filename in os.listdir(DIR_BRANCHES(base_dir)):
            branch_dir = os.path.join(DIR_BRANCHES(base_dir), filename)
            branches.append(
                FSBranchHandle(
                    base_dir=branch_dir,
                    workflows_dir=DIR_WORKFLOWS(base_dir),
                    modules_dir=DIR_MODULE(base_dir),
                    object_store=object_store
                )
            )
        # Return handle for new viztrail
        return FSViztrailHandle(
            identifier=identifier,
            exec_env_id=exec_env_id,
            properties=PersistentAnnotationSet(
                filename=FILE_PROPERTIES(base_dir)
            ),
            branches=branches,
            created_at=created_at,
            base_dir=base_dir,
            object_store=object_store
        )


# ------------------------------------------------------------------------------
# Files and Directories
# ------------------------------------------------------------------------------


def FILE_PROPERTIES(base_dir):
    """Get the absolute path to the file containing viztrail annotations.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'properties.json')


def FILE_VIZTRAIL(base_dir):
    """Get the absolute path to the file containing viztrail metadata.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'viztrail.json')


def DIR_BRANCHES(base_dir):
    """Get the absolute path to the subfolder containing viztrail branches.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'branches')


def DIR_MODULES(base_dir):
    """Get the absolute path to the subfolder containing workflow modules.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'modules')


def DIR_WORKFLOWS(base_dir):
    """Get the absolute path to the subfolder containing workflow versions.

    Parameters
    ----------
    base_dir: string
        Path to the base directory for the viztrail

    Returns
    -------
    string
    """
    return os.path.join(base_dir, 'workflows')
