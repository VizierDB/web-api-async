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

"""A workflow is a sequence of modules. Each module is represented by a module
handle. The handle maintains information about the module command, status, the
module outputs, and the module state (datasets).
"""


class ModuleProvenance(object):
    """The module provenance object maintains information about the datasets
    that a module has read and written in previous executions. Provenance
    information is maintained in two dictionaries where the key is the dataset
    name and the value the dataset identifier.

    Note that the dataset identifier that is associated with a name in either
    of the two maintained dictionaries may be None. This situation results for
    example from the executio of a python cell that attempted to read a dataset
    that did not exist or create a dataset that already existed.

    If provenance information is unknown (e.g., because the module has not been
    executed yet or it is treated as a black box) both dictionaries are None.

    Use method .requires_exec() if a module needs to be executed for a given
    database state.

    The module provenance carries a dictionary of key,value-pairs (resources)
    that were generated during previous executions of the module. This allows
    in a limited way to pass some state in between re-executions of the same
    module. THe main intent is to pass information about downloaded files and
    the resulting dataset identifier to avoid re-downloading the files but to
    use the previously generated local copy instead.
    """
    def __init__(self, read=None, write=None, resources=None):
        """Initialize the datasets that were read and written by a previous
        module execution.

        Parameters
        ----------
        read: dict, optional
            Dictionary of datasets that the module used as input
        write: dict, optional
            Dictionary of datasets that the module modified
        resources: dict, optional
            Resources and other information that was generated during execution
        """
        self.read = read
        self.write = write
        self.resources = resources

    def adjust_state(self, datasets, datastore):
        """Adjust a given database state by adding/replacing datasets in the
        given dictionary with those in the write dependencies of this provenance
        object.

        Parameters
        ----------
        datasets: dict(vizier.datastore.dataset.DatasetDescriptor)
            Dictionary of identifier for datasets in the current state. The key
            is the dataset name.
        datastore: vizier.datastore.base.Datastore
            Datastore to access descriptors for previously generated datasets.

        Returns
        -------
        dict(vizier.datastore.dataset.DatasetDescriptor)
        """
        if not self.write is None:
            result = dict(datasets)
            for ds_name in self.write:
                result[ds_name] = repository.get_descriptor(self.write[ds_name])
            return result
        else:
            return datasets

    def requires_exec(self, datasets):
        """Test if a module requires execution based on the provenance
        information and a given database state. If True, the module needs to be
        re-executes. Otherwise, the write dependencies can be copied to the new
        database state.

        If either of the read/write dependencies is None we always excute the
        module. Execution can be skipped if all previous inputs are present in
        the database state and if the module at most modifies the datasets that
        are in the set of read dependencies.

        Note that the identifier that are associated with dataset names may be
        None in the provenance information.

        Parameters
        ----------
        datasets: dict(vizier.datastore.dataset.DatasetDescriptor)
            Dictionary of identifier for datasets in the current state. The key
            is the dataset name.

        Returns
        -------
        bool
        """
        # Always execute if any of the provenance information for the module is
        # unknown (i.e., None)
        if self.read is None or self.write is None:
            return True
        # Always execute if the module unsuccessfully attempted to write a
        # dataset or if it creates or changes a dataset that is not
        # in the read dependencies but that exists in the current state
        for name in self.write:
            if self.write[name] is None:
                return True
            elif name in datasets and not name in self.read:
                return True
        # Check if all read dependencies are present and have not been modified
        for name in self.read:
            if not name in datasets:
                return True
            elif self.read[name] is None:
                return True
            elif self.read[name] != datasets[name].identifier:
                return True
        # The database state is the same as for the previous execution of the
        # module (with respect to the input dependencies). Thus, the module
        # does not need to be re-executed.
        return False
