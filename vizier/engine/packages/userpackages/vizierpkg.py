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

import copy
import csv
import sys
import urllib

from StringIO import StringIO

from vistrails.core.modules.vistrails_module import Module, NotCacheable

import vistrails.packages.mimir.init as mimir

from vizier.core.util import is_valid_name, get_unique_identifier
from vizier.datastore.fs import FileSystemDataStore
from vizier.datastore.mem import VolatileDataStore
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.mimir import COL_PREFIX, ROW_ID
from vizier.datastore.mimir import MimirDatasetColumn
from vizier.datastore.mimir import MimirDataStore, create_missing_key_view
from vizier.filestore.base import DefaultFileServer
from vizier.plot.view import ChartViewHandle
from vizier.serialize import CHART_VIEW, PLAIN_TEXT
from vizier.workflow.context import VizierDBClient
from vizier.workflow.module import ModuleOutputs
from vizier.workflow.vizual.base import DefaultVizualEngine
from vizier.workflow.vizual.mimir import MimirVizualEngine

import vizier.config as config
import vizier.workflow.command as cmd
import vizier.workflow.context as ctx


name = 'Vizier'
identifier = 'org.vistrails.vistrails.vizier'
version = '0.1'


class FakeStream(object):
    def __init__(self, tag, stream):
        self.closed = False
        self._tag = tag
        self._stream = stream

    def close(self):
        self.closed = True

    def flush(self):
        pass

    def writelines(self, iterable):
        for text in iterable:
            self.write(text)

    def write(self, text):
        if self._stream and self._stream[-1][0] == self._tag:
            self._stream[-1][1].append(text)
        else:
            self._stream.append((self._tag, [text]))


class MimirLens(Module):
    """Creates a Lens in mimir specific type."""
    _input_ports = [
        ('name', 'basic:String'),
        ('arguments', 'basic:Dictionary'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('command', 'basic:String'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        # Get input arguments
        lens = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Get command text. Catch potential exceptions (bugs) to avoid that the
        # workflow execution breaks here.
        #try:
        cmd_text = self.to_string(lens, args, vizierdb)
        #except Exception as ex:
        #    cmd_text = 'MIMIR ' + str(name)
        self.set_output('command', cmd_text)
        # Module outputs
        outputs = ModuleOutputs()
        store_as_dataset = None
        update_rows = False
        # Get dataset. Raise exception if dataset is unknown
        ds_name = get_argument(cmd.PARA_DATASET, args).lower()
        dataset_id = vizierdb.get_dataset_identifier(ds_name)
        dataset = vizierdb.datastore.get_dataset(dataset_id)
        if dataset is None:
            raise ValueError('unknown dataset \'' + ds_name + '\'')
        mimir_table_name = dataset.table_name
        if lens == cmd.MIMIR_DOMAIN:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = [column.name_in_rdb]
        elif lens == cmd.MIMIR_GEOCODE:
            geocoder = get_argument(cmd.PARA_GEOCODER, args)
            params = ['GEOCODER(' + geocoder + ')']
            add_param(params, 'HOUSE_NUMBER', dataset, args, cmd.PARA_HOUSE_NUMBER)
            add_param(params, 'STREET', dataset, args, cmd.PARA_STREET)
            add_param(params, 'CITY', dataset, args, cmd.PARA_CITY)
            add_param(params, 'STATE', dataset, args, cmd.PARA_STATE)
            # Add columns for LATITUDE and LONGITUDE
            c_id = dataset.column_counter
            c_lat = COL_PREFIX + str(c_id)
            dataset.columns.append(MimirDatasetColumn(c_id, 'LATITUDE', c_lat))
            dataset.column_counter += 1
            c_id = dataset.column_counter
            c_lon = COL_PREFIX + str(c_id)
            dataset.columns.append(MimirDatasetColumn(c_id, 'LONGITUDE', c_lon))
            dataset.column_counter += 1
            params.append('RESULT_COLUMNS(' + c_lat + ',' + c_lon + ')')
        elif lens == cmd.MIMIR_KEY_REPAIR:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = [column.name_in_rdb]
            update_rows = True
        elif lens == cmd.MIMIR_MISSING_KEY:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = [column.name_in_rdb]
            # Set MISSING ONLY to FALSE to ensure that all rows are returned
            params += ['MISSING_ONLY(FALSE)']
            # Need to run this lens twice in order to generate row ids for
            # any potential new tuple
            mimir_table_name = mimir._mimir.createLens(
                dataset.table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens,
                get_argument(cmd.PARA_MAKE_CERTAIN, args),
                False
            )
            params = [ROW_ID, 'MISSING_ONLY(FALSE)']
            update_rows = True
        elif lens == cmd.MIMIR_MISSING_VALUE:
            column = get_column_argument(dataset, args, cmd.PARA_COLUMN)
            params = column.name_in_rdb
            if cmd.PARA_CONSTRAINT in args:
                params = params + ' ' + str(args[cmd.PARA_CONSTRAINT])
                params = '\'' + params + '\''
            params = [params]
        elif lens == cmd.MIMIR_PICKER:
            pick_from = list()
            column_names = list()
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_col = get_argument(cmd.PARA_PICKFROM, col, as_int=True)
                column = dataset.column_by_id(c_col)
                pick_from.append(column.name_in_rdb)
                column_names.append(column.name.upper().replace(' ', '_'))
            # Add result column to dataset schema
            pick_as = ''
            if cmd.PARA_PICKAS in args:
                pick_as = args[cmd.PARA_PICKAS].strip()
            if pick_as == '':
                pick_as = 'PICK_ONE_' + '_'.join(column_names)
            target_column = COL_PREFIX + str(dataset.column_counter)
            dataset.columns.append(
                MimirDatasetColumn(
                    dataset.column_counter,
                    pick_as,
                    target_column
                )
            )
            dataset.column_counter += 1
            params = ['PICK_FROM(' + ','.join(pick_from) + ')']
            params.append('PICK_AS(' + target_column + ')')
        elif lens == cmd.MIMIR_SCHEMA_MATCHING:
            store_as_dataset = get_argument(cmd.PARA_RESULT_DATASET, args)
            if vizierdb.has_dataset_identifier(store_as_dataset):
                raise ValueError('dataset \'' + store_as_dataset + '\' exists')
            if not is_valid_name(store_as_dataset):
                raise ValueError('invalid dataset name \'' + store_as_dataset + '\'')
            column_names = list()
            params = ['\'' + ROW_ID + ' int\'']
            for col in get_argument(cmd.PARA_SCHEMA, args):
                c_name = get_argument(cmd.PARA_COLUMN, col)
                c_type = get_argument(cmd.PARA_TYPE, col)
                params.append('\'' + COL_PREFIX + str(len(column_names)) + ' ' + c_type + '\'')
                column_names.append(c_name)
        elif lens == cmd.MIMIR_TYPE_INFERENCE:
            params = [str(get_argument(cmd.PARA_PERCENT_CONFORM, args))]
        else:
            raise ValueError('unknown Mimir lens \'' + str(lens) + '\'')
        # Create Mimir lens
        if lens in [cmd.MIMIR_SCHEMA_MATCHING, cmd.MIMIR_TYPE_INFERENCE]:
            lens_name = mimir._mimir.createAdaptiveSchema(
                mimir_table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens
            )
        else:
            lens_name = mimir._mimir.createLens(
                mimir_table_name,
                mimir._jvmhelper.to_scala_seq(params),
                lens,
                get_argument(cmd.PARA_MAKE_CERTAIN, args),
                False
            )
        # Create a view including missing row ids for the result of a
        # MISSING KEY lens
        if lens == cmd.MIMIR_MISSING_KEY:
            lens_name, row_counter = create_missing_key_view(
                dataset,
                lens_name,
                column
            )
            dataset.row_counter = row_counter
        # Create datastore entry for lens.
        if not store_as_dataset is None:
            columns = list()
            for c_name in column_names:
                col_id = len(columns)
                columns.append(MimirDatasetColumn(
                    col_id,
                    c_name,
                    COL_PREFIX + str(col_id)
                ))
            #ds = vizierdb.datastore.create_dataset(table_name, columns)
            ds = vizierdb.datastore.register_dataset(
                table_name=lens_name,
                columns=columns,
                row_ids=dataset.row_ids,
                annotations=dataset.annotations,
                update_rows=True
            )
            ds_name = store_as_dataset
        else:
            ds = vizierdb.datastore.register_dataset(
                table_name=lens_name,
                columns=dataset.columns,
                row_ids=dataset.row_ids,
                column_counter=dataset.column_counter,
                row_counter=dataset.row_counter,
                annotations=dataset.annotations,
                update_rows=update_rows
            )
        print_dataset_schema(outputs, ds_name, ds.columns)
        vizierdb.set_dataset_identifier(ds_name, ds.identifier)
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set the module outputs
        self.set_output('context', context)
        self.set_output('output', outputs)

    def to_string(self, lens, args, vizierdb):
        """Get string representation for the cell command.

        Parameters
        ----------
        lens: string
            Unique lens identifier
        args: dict
            Dictionary of command arguments
        vizierdb: vizier.workflow.context.VizierDBClient
            VizierDB client as returned by get_env()

        Returns
        -------
        string
        """
        ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
        if lens == cmd.MIMIR_KEY_REPAIR:
            # KEY REPAIR FOR <column> IN <dataset>
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'KEY REPAIR FOR',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'IN',
                format_str(ds_name.lower())
            ])
        elif lens == cmd.MIMIR_MISSING_KEY:
            # MISSING KEYS FOR <column> IN <dataset>
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'MISSING KEYS FOR',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'IN',
                format_str(ds_name.lower())
            ])
        elif lens == cmd.MIMIR_DOMAIN:
            # DOMAIN FOR <column> IN <dataset>
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'DOMAIN FOR',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'IN',
                format_str(ds_name.lower())
            ])
        elif lens == cmd.MIMIR_GEOCODE:
            tokens = [
                'GEOCODE',
            ]
            goecode_columns = list()
            if cmd.PARA_HOUSE_NUMBER in args:
                col_id = get_argument(cmd.PARA_HOUSE_NUMBER, args)
                col_name = format_str(get_column_name(ds_name, col_id, vizierdb))
                goecode_columns.append('HOUSE_NUMBER=' + col_name)
            if cmd.PARA_STREET in args:
                col_id = get_argument(cmd.PARA_STREET, args)
                col_name = format_str(get_column_name(ds_name, col_id, vizierdb))
                goecode_columns.append('STREET=' + col_name)
            if cmd.PARA_CITY in args:
                col_id = get_argument(cmd.PARA_CITY, args)
                col_name = format_str(get_column_name(ds_name, col_id, vizierdb))
                goecode_columns.append('CITY=' + col_name)
            if cmd.PARA_STATE in args:
                col_id = get_argument(cmd.PARA_STATE, args)
                col_name = format_str(get_column_name(ds_name, col_id, vizierdb))
                goecode_columns.append('STATE=' + col_name)
            if len(goecode_columns) > 0:
                tokens.append(','.join(goecode_columns))
            tokens = tokens + [
                format_str(ds_name.lower()),
                'USING',
                get_argument(cmd.PARA_GEOCODER, args)
            ]
            return ' '.join(tokens)
        elif lens == cmd.MIMIR_MISSING_VALUE:
            # MISSING VALUES FOR <column> IN <dataset> {WITH CONSTRAINT} <constraint>
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            cmd_text = ' '.join([
                'MISSING VALUES FOR',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'IN',
                format_str(ds_name.lower())
            ])
            if cmd.PARA_CONSTRAINT in args:
                constraint = args[cmd.PARA_CONSTRAINT]
                if constraint != '':
                    cmd_text += ' WITH CONSTRAINT ' + str(constraint)
            return cmd_text
        elif lens == cmd.MIMIR_PICKER:
            # PICK FROM <columns> {AS <name>} IN <dataset>
            pick_from = list()
            for col in get_argument(cmd.PARA_SCHEMA, args, default_value=[]):
                col_id = get_argument(cmd.PARA_PICKFROM, col, raise_error=False)
                c_name = format_str(get_column_name(ds_name, col_id, vizierdb))
                pick_from.append(format_str(c_name))
            # Add result column to dataset schema
            pick_as = ''
            if cmd.PARA_PICKAS in args:
                pick_as = format_str(args[cmd.PARA_PICKAS].strip())
            if pick_as != '':
                pick_as = ' AS ' + pick_as
            pick_from = ','.join(pick_from) + pick_as
            return ' '.join([
                'PICK FROM',
                pick_from,
                'IN',
                format_str(ds_name.lower())
            ])
        elif lens == cmd.MIMIR_SCHEMA_MATCHING:
            # SCHEMA MATCHING <dataset>(<columns>) AS <name>
            store_as = get_argument(cmd.PARA_RESULT_DATASET, args, default_value='?')
            ds_schema = list()
            for col in get_argument(cmd.PARA_SCHEMA, args, default_value=[]):
                c_name = get_argument(cmd.PARA_COLUMN, col, default_value='?')
                c_type = get_argument(cmd.PARA_TYPE, col, default_value='?')
                ds_schema.append(format_str(c_name) + ' ' + c_type)
            return ' '.join([
                'SCHEMA MATCHING',
                format_str(ds_name.lower()),
                '(' + ', '.join(ds_schema) + ')',
                'AS',
                format_str(store_as)
            ])
        elif lens == cmd.MIMIR_TYPE_INFERENCE:
            # TYPE INFERENCE FOR COLUMNS IN <dataset> WITH percent_conform = <value>
            return ' '.join([
                'TYPE INFERENCE FOR COLUMNS IN',
                format_str(ds_name.lower()),
                'WITH percent_conform =',
                str(get_argument(cmd.PARA_PERCENT_CONFORM, args, default_value='?'))
            ])
        # Default message for unknown command.
        return 'unknown Mimir lens \'' + str(lens) + '\''


class PlotCell(NotCacheable, Module):
    """Vistrails module to execute a plot command. Expects a command type (name)
    and a dictionary of arguments that specify the dataset and data series
    that go into in the generated plot.
    """
    _input_ports = [
        ('name', 'basic:String'),
        ('arguments', 'basic:Dictionary'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('command', 'basic:String'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        """Excute the specified plot command on the current database state.
        Will raise ValueError if the referenced datasets does not exist.
        """
        name = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        outputs = ModuleOutputs()
        # Get command text. Catch potential exceptions (bugs) to avoid that the
        # workflow execution breaks here.
        try:
            cmd_text = self.to_string(name, args, vizierdb)
        except Exception as ex:
            cmd_text = 'PLOT ' + str(name)
        self.set_output('command', cmd_text)
        if name == cmd.PLOT_SIMPLE_CHART:
            # Get dataset name and the associated dataset. This will raise an
            # exception if the dataset name is unknown.
            ds_name = get_argument(cmd.PARA_DATASET, args)
            dataset_id = vizierdb.get_dataset_identifier(ds_name)
            dataset = vizierdb.datastore.get_dataset(dataset_id)
            # Get user-provided name for the new chart and verify that it is a
            # valid name
            chart_name = get_argument(cmd.PARA_NAME, args)
            if not is_valid_name(chart_name):
                raise ValueError('invalid chart name \'' + chart_name + '\'')
            chart_type = get_argument(cmd.PARA_CHART_TYPE, args[cmd.PARA_CHART])
            grouped_chart = bool(get_argument(cmd.PARA_CHART_GROUPED, args[cmd.PARA_CHART]))
            # Create a new chart view handle and add the series definitions
            view = ChartViewHandle(
                dataset_name=ds_name,
                chart_name=chart_name,
                chart_type=chart_type,
                grouped_chart=grouped_chart
            )
            # The data series index for x-axis values is optional
            if cmd.PARA_XAXIS in args:
                x_axis = args[cmd.PARA_XAXIS]
                # X-Axis column may be empty. In that case, we ignore the
                # x-axis spec
                col_id = get_argument(
                    cmd.PARA_XAXIS + '_' + cmd.PARA_COLUMN,
                    x_axis,
                    as_int=True
                )
                if isinstance(col_id, int):
                    add_data_series(
                        view=view,
                        series_spec=x_axis,
                        dataset=dataset,
                        prefix=cmd.PARA_XAXIS
                    )
                    view.x_axis = 0
            # Definition of data series. Each series is a pair of column
            # identifier and a printable label.
            for data_series in get_argument(cmd.PARA_SERIES, args):
                add_data_series(
                    view=view,
                    series_spec=data_series,
                    dataset=dataset,
                    default_label='Series ' + str(len(view.data) + 1)
                )
            # Execute the query and get the result
            rows = vizierdb.datastore.get_dataset_chart(dataset_id, view)
            # Add chart view handle as module output
            outputs.stdout(content=CHART_VIEW(view, rows=rows))
        else:
            raise ValueError('unknown plot command \'' + str(name) + '\'')
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set the module outputs
        self.set_output('context', context)
        self.set_output('output', outputs)

    def to_string(self, name, args, vizierdb):
        """Get string representation for the cell command.

        Parameters
        ----------
        name: string
            Unique command identifier
        args: dict
            Dictionary of command arguments
        vizierdb: vizier.workflow.context.VizierDBClient
            VizierDB client as returned by get_env()

        Returns
        -------
        string
        """
        if name == cmd.PLOT_SIMPLE_CHART:
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join([
                'CREATE PLOT',
                format_str(get_argument(cmd.PARA_NAME, args), default_value='?'),
                'FOR',
                format_str(ds_name.lower())
            ])
        # Default message for unknown command.
        return 'unknown plot command \'' + str(name) + '\''


class PythonCell(NotCacheable, Module):
    _input_ports = [
        ('source', 'basic:String'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('command', 'basic:String'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        # Get Python source code that is execyted in this cell and the global
        # variables
        source = urllib.unquote(self.get_input('source'))
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Get Python variables from context and set the current vizier client
        variables = context[ctx.VZRENV_VARS]
        variables[ctx.VZRENV_VARS_DBCLIENT] = vizierdb
        # Redirect standard output and standard error
        out = sys.stdout
        err = sys.stderr
        stream = []
        sys.stdout = FakeStream('out', stream)
        sys.stderr = FakeStream('err', stream)
        # Run the Pyhton code
        try:
            exec source in variables, variables
        except Exception as ex:
            template = "{0}:{1!r}"
            message = template.format(type(ex).__name__, ex.args)
            sys.stderr.write(str(message) + '\n')
        finally:
            sys.stdout = out
            sys.stderr = err
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set module outputs
        outputs = ModuleOutputs()
        for tag, text in stream:
            text = ''.join(text).strip()
            if tag == 'out':
                outputs.stdout(content=PLAIN_TEXT(text))
            else:
                outputs.stderr(content=PLAIN_TEXT(text))
        self.set_output('context', context)
        self.set_output('command', source)
        self.set_output('output', outputs)


class VizualCell(NotCacheable, Module):
    """Vistrails module to execute VizUAL commands. Expects a command type
    (name)and a dictionary of arguments that specify the actual VizUAL command
    and its arguments. The context contains the dataset mapping and reference to
    the VizUAL engine.
    """
    _input_ports = [
        ('name', 'basic:String'),
        ('arguments', 'basic:Dictionary'),
        ('context', 'basic:Dictionary')
    ]
    _output_ports = [
        ('context', 'basic:Dictionary'),
        ('command', 'basic:String'),
        ('output', 'basic:Dictionary')
    ]

    def compute(self):
        """Excute the specified VizUAL command on the current database state.
        Will raise ValueError if the referenced datasets does not exist.
        """
        name = self.get_input('name')
        args = self.get_input('arguments')
        context = self.get_input('context')
        # Get module identifier and VizierDB client for current workflow state
        module_id = self.moduleInfo['moduleId']
        vizierdb = get_env(module_id, context)
        # Set VizUAL engine (shortcut)
        v_eng = vizierdb.vizual
        outputs = ModuleOutputs()
        # Get command text. Catch potential exceptions (bugs) to avoid that the
        # workflow execution breaks here.
        try:
            cmd_text = self.to_string(name, args, vizierdb)
        except Exception as ex:
            cmd_text = 'VIZUAL ' + str(name)
        self.set_output('command', cmd_text)
        if name == cmd.VIZUAL_DEL_COL:
            # Get dataset name, and column specification. Raise exception if
            # the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute delete column command and set number of affected
            # columns in output
            col_count, ds_id = v_eng.delete_column(ds, c_col)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' column deleted'))
        elif name == cmd.VIZUAL_DEL_ROW:
            # Get dataset name, and row index. Raise exception if the
            # specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = get_argument(cmd.PARA_ROW, args, as_int=True)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute delete row command and set number of affected rows in
            # output
            col_count, ds_id = v_eng.delete_row(ds, c_row)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' row deleted'))
        elif name == cmd.VIZUAL_DROP_DS:
            # Get dataset name and remove the associated entry from the
            # dictionary of datasets in the context. Will raise exception if the
            # specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            vizierdb.remove_dataset_identifier(ds_name)
            outputs.stdout(content=PLAIN_TEXT('1 dataset dropped'))
        elif name == cmd.VIZUAL_INS_COL:
            # Get dataset name, column index, and new column name. Raise
            # exception if the specified dataset does not exist or the
            # column position is not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            c_name = get_argument(cmd.PARA_NAME, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute insert column command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output
            col_count, ds_id = v_eng.insert_column(ds, c_pos, c_name)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' column inserted'))
        elif name == cmd.VIZUAL_INS_ROW:
            # Get dataset name, and row index. Raise exception if the
            # specified dataset does not exist or row index is not an int.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = int(get_argument(cmd.PARA_POSITION, args))
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute insert row command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            col_count, ds_id = v_eng.insert_row(ds, c_row)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' row inserted'))
        elif name == cmd.VIZUAL_LOAD:
            # Get the name of the file and dataset name from command
            # arguments. Raise exception if a dataset with the specified
            # name already exsists in the project or if the given name is
            # not a valid dataset name
            ds_file = get_argument(cmd.PARA_FILE, args)[cmd.PARA_FILEID]
            ds_name = get_argument(cmd.PARA_NAME, args).lower()
            if vizierdb.has_dataset_identifier(ds_name):
                raise ValueError('dataset \'' + ds_name + '\' exists')
            if not is_valid_name(ds_name):
                raise ValueError('invalid dataset name \'' + ds_name + '\'')
            # Execute VizUAL creat dataset command. Add new dataset to
            # dictionary and add dataset schema and row count to output
            ds = v_eng.load_dataset(ds_file)
            vizierdb.set_dataset_identifier(ds_name, ds.identifier)
            print_dataset_schema(outputs, ds_name, ds.columns)
            outputs.stdout(content=PLAIN_TEXT(str(ds.row_count) + ' row(s)'))
        elif name == cmd.VIZUAL_MOV_COL:
            # Get dataset name, column name, and target position. Raise
            # exception if the specified dataset does not exist or the
            # target position is not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute move column command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output
            col_count, ds_id = v_eng.move_column(ds, c_col, c_pos)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' column moved'))
        elif name == cmd.VIZUAL_MOV_ROW:
            # Get dataset name, row index, and target index. Raise exception
            # if the specified dataset does not exist or if either of the
            # row indexes are not an integer.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_row = int(get_argument(cmd.PARA_ROW, args))
            c_pos = int(get_argument(cmd.PARA_POSITION, args))
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute insert row command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            col_count, ds_id = v_eng.move_row(ds, c_row, c_pos)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' row moved'))
        elif name == cmd.VIZUAL_PROJECTION:
            # Get the name of the dataset and the list of columns to filter
            # as well as the optional new column name.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            ds = vizierdb.get_dataset_identifier(ds_name)
            columns = list()
            names = list()
            for col in get_argument(cmd.PARA_COLUMNS, args):
                f_col = get_argument(cmd.PARA_COLUMNS_COLUMN, col, as_int=True)
                columns.append(f_col)
                col_name = None
                if cmd.PARA_COLUMNS_RENAME in col:
                    col_name = get_argument(cmd.PARA_COLUMNS_RENAME, col)
                if col_name == '':
                    col_name = None
                names.append(col_name)
            _, ds_id = v_eng.filter_columns(ds, columns, names)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(len(columns)) + ' column(s) filtered'))
        elif name == cmd.VIZUAL_REN_COL:
            # Get dataset name, column specification, and new column nmae.
            # Raise exception if the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_name = get_argument(cmd.PARA_NAME, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute rename colum command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # columns in output.
            col_count, ds_id = v_eng.rename_column(ds, c_col, c_name)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(col_count) + ' column renamed'))
        elif name == cmd.VIZUAL_REN_DS:
            # Get name of existing dataset and the new dataset name. Raise
            # exception if the specified dataset does not exist, a dataset with
            # the new name already exists, or if the new dataset name is not a
            # valid name.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            new_name = get_argument(cmd.PARA_NAME, args)
            if vizierdb.has_dataset_identifier(new_name):
                raise ValueError('dataset \'' + new_name + '\' exists')
            if not is_valid_name(new_name):
                raise ValueError('invalid dataset name \'' + new_name + '\'')
            ds = vizierdb.get_dataset_identifier(ds_name)
            vizierdb.remove_dataset_identifier(ds_name)
            vizierdb.set_dataset_identifier(new_name, ds)
            outputs.stdout(content=PLAIN_TEXT('1 dataset renamed'))
        elif name == cmd.VIZUAL_SORT:
            # Get the name of the dataset and the list of columns to sort on
            # as well as the optional sort order.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            ds = vizierdb.get_dataset_identifier(ds_name)
            columns = list()
            reversed = list()
            for col in get_argument(cmd.PARA_COLUMNS, args):
                s_col = get_argument(cmd.PARA_COLUMNS_COLUMN, col, as_int=True)
                columns.append(s_col)
                sort_order = get_argument(cmd.PARA_COLUMNS_ORDER, col)
                if sort_order == cmd.SORT_DESC:
                    reversed.append(True)
                else:
                    reversed.append(False)
            count, ds_id = v_eng.sort_dataset(ds, columns, reversed)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(count) + ' row(s) sorted'))
        elif name == cmd.VIZUAL_UPD_CELL:
            # Get dataset name, cell coordinates, and update value. Raise
            # exception if the specified dataset does not exist.
            ds_name = get_argument(cmd.PARA_DATASET, args).lower()
            c_col = get_argument(cmd.PARA_COLUMN, args, as_int=True)
            c_row = get_argument(cmd.PARA_ROW, args, as_int=True)
            c_val = get_argument(cmd.PARA_VALUE, args)
            ds = vizierdb.get_dataset_identifier(ds_name)
            # Execute update cell command. Replacte existing dataset
            # identifier with updated dataset id and set number of affected
            # rows in output
            upd_count, ds_id = v_eng.update_cell(ds, c_col, c_row, c_val)
            vizierdb.set_dataset_identifier(ds_name, ds_id)
            outputs.stdout(content=PLAIN_TEXT(str(upd_count) + ' row updated'))
        else:
            raise ValueError('unknown vizual command \'' + str(name) + '\'')
        # Propagate potential changes to the dataset mappings
        propagate_changes(module_id, vizierdb.datasets, context)
        # Set the module outputs
        self.set_output('context', context)
        self.set_output('output', outputs)

    def to_string(self, name, args, vizierdb):
        """Get string representation for the cell command.

        Parameters
        ----------
        name: string
            Unique command identifier
        args: dict
            Dictionary of command arguments
        vizierdb: vizier.workflow.context.VizierDBClient
            VizierDB client as returned by get_env()

        Returns
        -------
        string
        """
        if name in [cmd.VIZUAL_DEL_COL]:
            # DELETE COLUMN <name> FROM <dataset>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'DELETE COLUMN',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'FROM',
                format_str(ds_name.lower())
            ])
        elif name == cmd.VIZUAL_DEL_ROW:
            # DELETE ROW <index> FROM <dataset>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join([
                'DELETE ROW',
                str(get_argument(cmd.PARA_ROW, args, default_value='?')),
                'FROM',
                format_str(ds_name.lower())
            ])
        elif name == cmd.VIZUAL_DROP_DS:
            # DROP DATASET <dataset>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join(['DROP DATASET', format_str(ds_name.lower())])
        elif name == cmd.VIZUAL_INS_COL:
            # INSERT COLUMN <name> INTO <dataset> AT POSITION <index>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join([
                'INSERT COLUMN',
                format_str(get_argument(cmd.PARA_NAME, args, default_value='?')),
                'INTO',
                format_str(ds_name.lower()),
                'AT POSITION',
                str(get_argument(cmd.PARA_POSITION, args, default_value='?'))
            ])
        elif name == cmd.VIZUAL_INS_ROW:
            # INSERT ROW INTO <dataset> AT POSITION <index>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join([
                'INSERT ROW INTO',
                format_str(ds_name.lower()),
                'AT POSITION',
                str(get_argument(cmd.PARA_POSITION, args, default_value='?'))
            ])
        elif name == cmd.VIZUAL_LOAD:
            # LOAD DATASET <dataset> FROM FILE <name>
            file_info = get_argument(cmd.PARA_FILE, args, default_value='?')
            if isinstance(file_info, dict):
                file_id = file_info[cmd.PARA_FILEID]
            else:
                file_id = file_info
            ds_name = get_argument(cmd.PARA_NAME, args, default_value='?')
            f_handle = vizierdb.vizual.fileserver.get_file(file_id)
            return ' '.join([
                'LOAD DATASET',
                format_str(ds_name),
                'FROM FILE',
                f_handle.source if not f_handle is None else '?'
            ])
        elif name == cmd.VIZUAL_MOV_COL:
            # MOVE COLUMN <name> IN <dataset> TO POSITION <index>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'MOVE COLUMN',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'IN',
                format_str(ds_name.lower()),
                'TO POSITION',
                str(get_argument(cmd.PARA_POSITION, args, default_value='?'))
            ])
        elif name == cmd.VIZUAL_MOV_ROW:
            # MOVE ROW <index> IN <dataset> TO POSITION <index>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join([
                'MOVE ROW',
                str(get_argument(cmd.PARA_ROW, args, default_value='?')),
                'IN',
                format_str(ds_name.lower()),
                'TO POSITION',
                str(get_argument(cmd.PARA_POSITION, args, default_value='?'))
            ])
        elif name == cmd.VIZUAL_PROJECTION:
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            filter_columns = list()
            for col in get_argument(cmd.PARA_COLUMNS, args, default_value=list()):
                col_id = get_argument(
                    cmd.PARA_COLUMNS_COLUMN, col, default_value='?'
                )
                col_name = get_column_name(ds_name, col_id, vizierdb)
                col_name = format_str(col_name)
                new_name = get_argument(cmd.PARA_COLUMNS_RENAME, col)
                if new_name != '':
                    col_name += ' AS ' + format_str(new_name)
                filter_columns.append(col_name)
            return ' '.join([
                'FILTER COLUMNS',
                ', '.join(filter_columns),
                'FROM',
                format_str(ds_name.lower())
            ])
        elif name == cmd.VIZUAL_REN_COL:
            # RENAME COLUMN <name> IN <dataset> TO <name>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'RENAME COLUMN',
                format_str(get_column_name(ds_name, col_id, vizierdb)),
                'IN',
                format_str(ds_name.lower()),
                'TO',
                format_str(get_argument(cmd.PARA_NAME, args, default_value='?'))
            ])
        elif name == cmd.VIZUAL_REN_DS:
            # RENAME DATASET <dataset> TO <name>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            return ' '.join([
                'RENAME DATASET',
                format_str(ds_name.lower()),
                'TO',
                format_str(get_argument(cmd.PARA_NAME, args, default_value='?'))
            ])
        elif name == cmd.VIZUAL_SORT:
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            sort_columns = list()
            for col in get_argument(cmd.PARA_COLUMNS, args, default_value=list()):
                col_id = get_argument(
                    cmd.PARA_COLUMNS_COLUMN, col, default_value='?'
                )
                col_name = get_column_name(ds_name, col_id, vizierdb)
                col_name = format_str(col_name)
                sort_order = get_argument(cmd.PARA_COLUMNS_ORDER, col)
                if sort_order != '':
                    col_name += ' (' + sort_order + ')'
                sort_columns.append(col_name)
            return ' '.join([
                'SORT',
                format_str(ds_name.lower()),
                'BY',
                ', '.join(sort_columns)
            ])
        elif name == cmd.VIZUAL_UPD_CELL:
            # UPDATE <dataset> SET [<column>,<row>] = <value>
            ds_name = get_argument(cmd.PARA_DATASET, args, raise_error=False)
            col_id = get_argument(cmd.PARA_COLUMN, args, raise_error=False)
            return ' '.join([
                'UPDATE',
                format_str(ds_name.lower()),
                'SET',
                ''.join([
                    '[',
                    format_str(get_column_name(ds_name, col_id, vizierdb)),
                    ',',
                    str(get_argument(cmd.PARA_ROW, args, default_value='?')),
                    ']'
                ]),
                '=',
                number_or_str(
                    get_argument(cmd.PARA_VALUE, args, default_value='?')
                )
            ])
        # Default message for unknown command.
        return 'unknown vizual command \'' + str(name) + '\''


# ------------------------------------------------------------------------------
#
# VizUAL Helper Methods
#
# ------------------------------------------------------------------------------

def add_data_series(view, series_spec, dataset, prefix=cmd.PARA_SERIES, default_label=None):
    """Add a data series handle to a given chart view handle. Expects a data
    series specification and a dataset descriptor.

    Parameters
    ----------
    view: vizier.plot.view.ChartViewHandle
        Chart view handle
    series_spec: dict()
        Data series specification
    dataset: vizier.datastore.base.DatasetHandle
        Dataset handle
    prefix: string, optional
        Prefix for all arguments in the data series specification.
    default_label: string, optional
        Default label for dataseries if not given in the specification.
    """
    col_id = get_argument(prefix + '_' + cmd.PARA_COLUMN, series_spec, as_int=True)
    # Get column index to ensure that the column exists. Will raise
    # an exception if c_name does not specify a valid column.
    c_name = dataset.column_by_id(col_id).name
    if prefix + '_' + cmd.PARA_LABEL in series_spec:
        s_label = str(series_spec[prefix + '_' + cmd.PARA_LABEL])
        if s_label.strip() == '':
            s_label = default_label if not default_label is None else c_name
    else:
        s_label = default_label if not default_label is None else c_name
    # Check for range specifications. Expect string of format int or
    # int:int with the second value being greater or equal than
    # the first.
    range_start = None
    range_end = None
    if prefix + '_' + cmd.PARA_RANGE in series_spec:
        s_range = series_spec[prefix + '_' + cmd.PARA_RANGE].strip()
        if s_range != '':
            pos = s_range.find(':')
            if pos > 0:
                range_start = int(s_range[:pos])
                range_end = int(s_range[pos+1:])
                if range_start > range_end:
                    raise ValueError('invalid range \'' + s_range + '\'')
            else:
                range_start = int(s_range)
                range_end = range_start
            if range_start < 0 or range_end < 0:
                raise ValueError('invalid range \'' + s_range + '\'')
    view.add_series(
        column=col_id,
        label=s_label,
        range_start=range_start,
        range_end=range_end
    )


def add_param(params, name, dataset, args, key):
    """Add a Mimir lens parameter to the given list. This is a shortcut to add
    a column parameter to a list of arguments for a lens.

    params: list
        List of Mimir lens parameters
    name: string
        Name of the parameter
    dataset: vizier.datastore.base.DatasetHandle
        Dataset handle
    args : dict(dict())
        Dictionary of command arguments
    key : string
        Argument name
    """
    column = get_column_argument(dataset, args, key, optional=True)
    if not column is None:
        params.append(name + '(' + column.name_in_rdb + ')')

def format_str(value, default_value='?'):
    """Format an output string. Simply puts single quotes around the value if
    it contains a space character. If the given argument is none the default
    value is returned

    Parameters
    ----------
    value: string
        Given input value. Expected to be of type string
    default_value: string
        Default value that is returned in case value is None

    Returns
    -------
    string
    """
    if value is None:
        return default_value
    if ' ' in str(value):
        return '\'' + str(value) + '\''
    else:
        return str(value)


def get_argument(key, args, as_int=False, raise_error=True, default_value=None):
    """Retrieve command argument with given key. Will raise ValueError if no
    argument with given key is present.

    Parameters
    ----------
    key : string
        Argument name
    args : dict(dict())
        Dictionary of command arguments
    as_int : bool
        Flag indicating whther the argument should be cobverted to int. If the
        given value cannot be converted the original argument value is returned.
    raise_error: bool, optional
        Flag indicating whether to raise a ValueError if the args dictionary
        does not contain key. If False the default_value will be returned
        instead. Is ignored if default_value is not None (implies raise_error to
        be True)
    default_value: string, optional
        Default value that is returned if the args dictionary does not contain
        key

    Returns
    -------
    dict
    """
    if not key in args:
        if raise_error and default_value is None:
            raise ValueError('missing argument \'' + key + '\'')
        else:
            return default_value
    val = args[key]
    if as_int:
        try:
            val = int(val)
        except ValueError as ex:
            pass
    return val


def get_column_argument(dataset, args, key, optional=False):
    """Get a datast column object specified by a command argument parameter.
    Assumes that the column is specified by the column identifier.

    Raises ValueError if the argument is not optional and no value for the given
    key exists or the value cannot be converted to an integer.

    Parameters
    ----------
    dataset: vizier.datastore.base.DatasetHandle
        Dataset handle
    args : dict(dict())
        Dictionary of command arguments
    key : string
        Argument name
    optional : bool, Optional
        Flag indicating whther the argument is optional.

    Returns
    -------
    vizier.datastore.base.DatasetColumn
    """
    # Return None if the argument is optional and not present in the given
    # dictionary.
    if optional and not key in args:
        return None
    c_col = get_argument(key, args, as_int=True)
    return dataset.column_by_id(c_col)


def get_column_name(ds_name, column_id, vizierdb):
    """Get the name of a column with given id in the dataset with ds_name from
    the current context. Returns col_id if the respective column is not found.

    Parameters
    ----------
    ds_name: string or None
        Name of the dataset (or None if argument was missing)
    column_id: str or int or None
        Identifier of dataset column (may be missing)
    vizierdb: vizier.workflow.context.VizierDBClient
        VizierDB client as returned by get_env()

    Returns
    -------
    string
    """
    try:
        # Try to convert col_id to int (as this is not guaranteed)
        col_id = int(column_id)
        # Get descriptor for dataset with given name
        ds = vizierdb.get_dataset(ds_name)
        for col in ds.columns:
            if col.identifier == col_id:
                return col.name
    except Exception:
        pass
    return str(column_id)


def get_env(module_id, context):
    """Get the VizierDB client for the workflow state of the given module.

    Patameters
    ----------
    module_id: int
        Unique module identifier
    context: dict
        Workflow execution context

    Returns
    -------
    vizier.workflow.context.VizierDBClient
    """
    # Get context type, environment type, and dataset mappings from the context
    # dictionary
    context_type = context[ctx.VZRENV_TYPE]
    env_type = context[ctx.VZRENV_ENV][ctx.VZRENV_ENV_IDENTIFIER]
    # Get dataset mapping for the given module. Note that the input datasets for
    # a module is the set of datasets that are in the state of the previous
    # module.
    datasets = None
    prev_map = None
    for module_map in context[ctx.VZRENV_DATASETS]:
        if module_map[ctx.VZRENV_DATASETS_MODULEID] == module_id:
            # Copy dataset mapping from previous module
            if not prev_map is None:
                datasets = dict(prev_map[ctx.VZRENV_DATASETS_MAPPING])
            else:
                datasets = dict()
            break
        prev_map = module_map
    # Get file server and datastore directories
    datastore_dir = context[ctx.VZRENV_ENV][ctx.VZRENV_ENV_DATASTORE]
    fileserver_dir = context[ctx.VZRENV_ENV][ctx.VZRENV_ENV_FILESERVER]
    # Use the default file server for vizual engine
    fileserver = DefaultFileServer(fileserver_dir)
    # Create the datastore. Use a volatile store if the context is volatile
    datastore = None
    if env_type == config.ENGINEENV_DEFAULT:
        datastore = FileSystemDataStore(datastore_dir)
    elif env_type == config.ENGINEENV_MIMIR:
        datastore = MimirDataStore(datastore_dir)
    if context_type == ctx.CONTEXT_VOLATILE:
        datastore = VolatileDataStore(datastore)
    # Create Viual engine depending on environment type
    vizual = None
    if env_type == config.ENGINEENV_DEFAULT:
        vizual = DefaultVizualEngine(datastore, fileserver)
    elif env_type == config.ENGINEENV_MIMIR:
        vizual = MimirVizualEngine(datastore, fileserver)
    # Return vizier client
    return VizierDBClient(datastore, datasets, vizual)


def number_or_str(value):
    """Puts the value into single quotes if it cannot be converted into a
    number.

    Parameters
    ----------
    value: string or number
        Input value

    Returns
    -------
    string
    """
    # Return the string NULL if the given value is None
    if value is None:
        return 'NULL'
    try:
        val = int(value)
        return str(val)
    except ValueError:
        try:
            val = float(value)
            return str(val)
        except ValueError:
            pass
    return '\'' + str(value) + '\''


def print_dataset_schema(outputs, name, columns):
    """Add schema infromation for given dataset to cell output.

    Parameters
    ----------
    outputs: vizier.workflow.module.ModuleOutputs
        Cell outputt streams
    name: string
        Dataset name
    columns: list(vizier.datasetore.base.DatasetColumn)
        Columns in the dataset schema
    """
    outputs.stdout(content=PLAIN_TEXT(name + ' ('))
    for i in range(len(columns)):
        text = '  ' + str(columns[i])
        if i != len(columns) - 1:
            text += ','
        outputs.stdout(content=PLAIN_TEXT(text))
    outputs.stdout(content=PLAIN_TEXT(')'))


def propagate_changes(module_id, datasets, context):
    """After executing the module, identify potential changes and propagate them
    to the dataset mappings in the global workflow context.

    Parameters
    ----------
    module_id: int
        Unique module identifier
    datasets: dict
        Dataset name to identifier mapping for module after execution finished
    context: dict
        Global workflow context
    """
    # Only propagate if not volatile
    if context[ctx.VZRENV_TYPE] != ctx.CONTEXT_VOLATILE:
        mappings = context[ctx.VZRENV_DATASETS]
        for i in range(len(mappings)):
            m_map = mappings[i]
            if m_map[ctx.VZRENV_DATASETS_MODULEID] == module_id:
                m_map[ctx.VZRENV_DATASETS_MAPPING] = datasets
                if i < len(mappings) - 1:
                    mappings[i+1][ctx.VZRENV_DATASETS_MAPPING] = dict(datasets)
                # Clear the datasets for the remaining modules
                for j in range(i+2,len(mappings)):
                    mappings[j][ctx.VZRENV_DATASETS_MAPPING] = dict()
                break


# Package modules
_modules = [MimirLens, PythonCell, VizualCell]
