"""Test functionality of the file system viztrail repository."""

import os
import shutil
import unittest


from vizier.engine.packages.pycell.command import python_cell
from vizier.core.timestamp import get_current_time
from vizier.datastore.dataset import DatasetColumn, DatasetDescriptor
from vizier.viztrail.objectstore.module import OSModuleHandle
from vizier.viztrail.objectstore.viztrail import OSViztrailHandle
from vizier.viztrail.module.base import MODULE_RUNNING, MODULE_SUCCESS
from vizier.viztrail.module.output import ModuleOutputs, TextOutput
from vizier.viztrail.module.provenance import ModuleProvenance
from vizier.viztrail.module.timestamp import ModuleTimestamp
from vizier.viztrail.workflow import ACTION_INSERT


REPO_DIR = './.temp'


class TestOSWorkflow(unittest.TestCase):

    def setUp(self):
        """Create an empty repository directory."""
        if os.path.isdir(REPO_DIR):
            shutil.rmtree(REPO_DIR)
        os.makedirs(REPO_DIR)

    def tearDown(self):
        """Delete repository directory.
        """
        shutil.rmtree(REPO_DIR)

    def test_load_active(self):
        """Test loading workflows with active modules."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(5):
            ts = get_current_time()
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                datasets=dict(),
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
            self.assertEqual(len(branch.get_history()), (i + 1))
        # This is a hack to simulate loading workflows with active modules
        # Change state of last two modules in branch head to an active state
        m = branch.get_head().modules[-2]
        m.state = MODULE_RUNNING
        m.write_module()
        m = branch.get_head().modules[-1]
        m.state = MODULE_RUNNING
        m.write_module()
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        self.assertTrue(branch.get_head().modules[0].is_success)
        self.assertTrue(branch.get_head().modules[1].is_success)
        self.assertTrue(branch.get_head().modules[2].is_success)
        self.assertTrue(branch.get_head().modules[3].is_canceled)
        self.assertTrue(branch.get_head().modules[4].is_canceled)
        # Change state of last module in second workflow to an active state
        m = branch.get_head().modules[1]
        m.state = MODULE_RUNNING
        m.write_module()
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        wf = branch.get_workflow(branch.get_history()[1].identifier)
        self.assertTrue(wf.modules[0].is_success)
        self.assertTrue(wf.modules[1].is_canceled)

    def test_load_with_dataset(self):
        """Test loading workflows where each module creates a new dataset."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(5):
            ts = get_current_time()
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(
                    write={'DS' + str(i): DatasetDescriptor(
                        identifier=str(i),
                        columns=[DatasetColumn(identifier=j, name=str(j)) for j in range(i)],
                        row_count=i
                    )}
                ),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
        vt = OSViztrailHandle.load_viztrail(base_path)
        workflow = vt.get_default_branch().get_head()
        self.assertEqual(len(workflow.modules), 5)
        for i in range(5):
            module = workflow.modules[i]
            self.assertEqual(len(module.datasets), i + 1)
            for j in range(i):
                key = 'DS' + str(j)
                self.assertTrue(key in module.datasets)
                self.assertEqual(len(module.datasets[key].columns), j)

    def test_load_with_dataset_delete(self):
        """Test loading workflows where each module creates a new dataset and
        deletes the previous dataset (except for the first module).
        """
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(5):
            ts = get_current_time()
            deleted_datasets = list()
            if i > 0:
                deleted_datasets.append('DS' + str(i-1))
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(
                    write={'DS' + str(i): DatasetDescriptor(
                        identifier=str(i),
                        columns=[DatasetColumn(identifier=j, name=str(j)) for j in range(i)],
                        row_count=i
                    )},
                    delete=deleted_datasets
                ),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
        vt = OSViztrailHandle.load_viztrail(base_path)
        workflow = vt.get_default_branch().get_head()
        self.assertEqual(len(workflow.modules), 5)
        for i in range(5):
            module = workflow.modules[i]
            self.assertEqual(len(module.datasets), 1)
            key = 'DS' + str(i)
            self.assertTrue(key in module.datasets)
            self.assertEqual(len(module.datasets[key].columns), i)

    def test_load_with_dataset_replace(self):
        """Test loading workflows where each module modifies a single dataset.
        """
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(5):
            ts = get_current_time()
            deleted_datasets = list()
            if i > 0:
                deleted_datasets.append('DS' + str(i-1))
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(
                    write={'DS': DatasetDescriptor(
                        identifier=str(i),
                        columns=[DatasetColumn(identifier=j, name=str(j)) for j in range(i)],
                        row_count=i
                    )}
                ),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
        vt = OSViztrailHandle.load_viztrail(base_path)
        workflow = vt.get_default_branch().get_head()
        self.assertEqual(len(workflow.modules), 5)
        for i in range(5):
            module = workflow.modules[i]
            self.assertEqual(len(module.datasets), 1)
            self.assertTrue('DS' in module.datasets)
            self.assertEqual(len(module.datasets['DS'].columns), i)

    def test_load_with_missing_modules(self):
        """Test loading workflows with active modules."""
        base_path = os.path.join(os.path.abspath(REPO_DIR), 'ABC')
        os.makedirs(base_path)
        vt = OSViztrailHandle.create_viztrail(
            identifier='ABC',
            properties={},
            base_path=base_path
        )
        branch = vt.get_default_branch()
        # Append ten modules
        for i in range(5):
            ts = get_current_time()
            command = python_cell(source='print ' + str(i) + '+' + str(i))
            module = OSModuleHandle.create_module(
                command=command,
                external_form='print ' + str(i) + '+' + str(i),
                state=MODULE_SUCCESS,
                datasets=dict(),
                outputs=ModuleOutputs(stdout=[TextOutput(str(i + i))]),
                provenance=ModuleProvenance(),
                timestamp=ModuleTimestamp(created_at=ts,started_at=ts,finished_at=ts),
                module_folder=vt.modules_folder,
                object_store=vt.object_store
            )
            if not branch.head is None:
                modules = branch.head.modules + [module]
            else:
                modules = [module]
            branch.append_workflow(
                modules=modules,
                action=ACTION_INSERT,
                command=command
            )
            self.assertEqual(len(branch.get_history()), (i + 1))
        # Delete the file for the third module to simulate an error condition in
        # which a file wasn't written properly
        os.remove(branch.head.modules[2].module_path)
        self.assertFalse(os.path.isfile(branch.head.modules[2].module_path))
        vt = OSViztrailHandle.load_viztrail(base_path)
        branch = vt.get_default_branch()
        self.assertTrue(branch.head.get_state().is_error)
        self.assertTrue(branch.head.modules[2].is_error)


if __name__ == '__main__':
    unittest.main()
