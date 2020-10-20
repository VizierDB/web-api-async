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

"""Classes to manipulate vizier datasets from within the Python workflow cell.
"""

from typing import Optional, TYPE_CHECKING, Dict, Any, List
if TYPE_CHECKING:
    from vizier.engine.packages.pycell.client.base import VizierDBClient

from vizier.datastore.dataset import DatasetColumn, DatasetRow, DatasetHandle
from vizier.datastore.base import get_column_index
from bokeh.models.sources import ColumnDataSource # type: ignore[import]


class DatasetClient(object):
    """Client to interact with a Vizier dataset from within a Python workflow
    cell. Provides access to the columns and rows. Allows to insert and delete
    rows, and to update cell values.

    Attributes
    ----------
    columns: list(vizier.datastore.base.DatasetColumns)
        List of dataset columns
    identifier : string
        Unique dataset identifier
    rows : list(vizier.datastore.client.MutableDatasetRow)
        List of rows in the dataset
    """
    def __init__(self, 
            dataset: Optional[DatasetHandle] = None, 
            client: Optional["VizierDBClient"] = None, 
            existing_name: str = None):
        """Initialize the client for a given dataset.

        Raises ValueError if dataset columns or rows do not have unique
        identifiers.

        Parameters
        ----------
        dataset: vizier.datastore.base.DatasetHandle, optional
            Handle to the dataset for which this is a client. If None this is a
            new dataset.
        client: vizier.engine.packages.pycell.client.VizierDBClient, optional
            Provide a client, to allow use of dataset.save(), dataset.save_as()
        existing_name: string, optional
            Provide an existing name, to allow use of dataset.save()
        """
        self.dataset = dataset
        self.client = client
        self.existing_name = existing_name
        if dataset is not None:
            self.identifier: Optional[str] = dataset.identifier
            self.columns = dataset.columns
            # Delay fetching rows and dataset annotations for now
            self._properties: Optional[Dict[str, Any]] = None
            self._rows: Optional[List[DatasetRow]] = None
        else:
            self.identifier = None
            self.columns = list()
            self._properties = {}
            self._rows = list()

    def __getitem__(self, key):
        return self.get_column(key)

    def save(self, name = None):
        if self.client is None:
            raise "Client field unset.  Use `vizierdb.create_dataset()` or `vizierdb.update_dataset()` instead."
        if name is None and self.existing_name is None:
            raise "This is a new dataset.  Use `ds.save(name = ...)` to specify a name."
        if name is None:
            self.client.update_dataset(name = self.existing_name, dataset = self)
        else:
            self.client.create_dataset(name = name, dataset = self)

    @property
    def properties(self):
        """Get all dataset properties

        Returns
        -------
        dict(string:any)
        """
        return self._properties or {}

    def column_index(self, column_id):
        """Get position of a given column in the dataset schema. The given
        column identifier could either be of type int (i.e., the index position
        of the column), or a string (either the column name or column label). If
        column_id is of type string it is first assumed to be a column name.
        Only if no column matches the column name or if multiple columns with
        the given name exist will the value of column_id be interpreted as a
        label.

        Raises ValueError if column_id does not reference an existing column in
        the dataset schema.

        Parameters
        ----------
        column_id : int or string
            Column index, name, or label

        Returns
        -------
        int
        """
        return get_column_index(self.columns, column_id)

    def delete_column(self, name):
        """Delete column from the dataset.

        Parameters
        ----------
        name: string or int
            Name or index position of the new column
        """
        # It is important to fetch the rows before the column is deleted.
        # Otherwise, the list of returned values per row will be missing the
        # value for the deleted columns (for Mimir datasets).
        ds_rows = self.rows
        col_index = self.column_index(name)
        # Delete column from schema
        del self.columns[col_index]
        # Delete all value for the deleted column
        for row in ds_rows:
            del row.values[col_index]

    def get_column(self, name):
        """Get the fist column in the dataset schema that matches the given
        name. If no column matches the given name None is returned.

        Parameters
        ----------
        name: string
            Column name

        Returns
        -------
        vizier.datastore.dataset.DatasetColumn
        """
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def insert_column(self, 
        name: str, 
        data_type: str = "varchar", 
        position: Optional[int] = None
    ) -> DatasetColumn:
        """Add a new column to the dataset schema.

        Parameters
        ----------
        name: string
            Name of the new column
        position: int, optional
            Position in the dataset schema where new column is inserted. If
            None, the column is appended to the list of dataset columns.

        Returns
        DatasetColumn
        """
        column = DatasetColumn(name=name, data_type = data_type)
        self.columns = list(self.columns)
        if not position is None:
            self.columns.insert(position, column)
            # Add a null value to each row for the new column
            for row in self.rows:
                row.values.insert(position, None)
        else:
            self.columns.append(column)
            # Add a null value to each row for the new column
            for row in self.rows:
                row.values.append(None)
        return column

    def insert_row(self, 
        values: Optional[List[Any]] = None, 
        position: Optional[int] = None
    ) -> DatasetRow:
        """Add a new row to the dataset. Expects a list of string values, one
        for each of the columns.

        Raises ValueError if the length of the values list does not match the
        number of columns in the dataset.

        Parameters
        ----------
        values: list(string), optional
            List of column values. Use empty string if no values are given
        position: int, optional
            Position where row is inserted. If None, the new row is appended to
            the list of dataset rows.

        Returns
        -------
        DatasetRow
        """
        # Ensure that there is exactly one value for each column in the dataset
        if not values is None:
            if len(values) != len(self.columns):
                raise ValueError('invalid number of values for dataset schema')
            row = MutableDatasetRow(
                values=[str(v) for v in values],
                dataset=self
            )
        else:
            # All values in the new row are set to the empty string by default.
            row = MutableDatasetRow(
                values = [None] * len(self.columns),
                dataset=self
            )
        if not position is None:
            self.rows.insert(position, row)
        else:
            self.rows.append(row)
        return row

    def get_cell(self, column, row):
        """Get dataset value for specified cell.

        Raises ValueError if [column, row] does not reference an existing cell.

        Parameters
        ----------
        column : int or string
            Column identifier
        row : int
            Row index

        Returns
        -------
        string
        """
        if row < 0 or row > len(self.rows):
            raise ValueError('unknown row \'' + str(row) + '\'')
        return self.rows[row].get_value(column)

    def move_column(self, name, position):
        """Move a column within a given dataset.

        Raises ValueError if no dataset with given identifier exists or if the
        specified column is unknown or the target position invalid.

        Parameters
        ----------
        name: string or int
            Name or index position of the new column
        position: int
            Target position for the column
        """
        # Get dataset. Raise exception if dataset is unknown
        # Make sure that position is a valid column index in the new dataset
        if position < 0 or position > len(self.columns):
            raise ValueError('invalid target position \'' + str(position) + '\'')
        # Get index position of column that is being moved
        source_idx = self.column_index(name)
        # No need to do anything if source position equals target position
        if source_idx != position:
            self.columns.insert(position, self.columns.pop(source_idx))
            for row in self.rows:
                row.values.insert(position, row.values.pop(source_idx))

    @property
    def rows(self):
        """Fetch rows on demand.

        Returns
        -------
        list(vizier.datastore.client.MutableDatasetRow)
        """
        if self._rows is None:
            self._rows = list()
            for row in self.dataset.fetch_rows():
                # Create mutable dataset row and set reference to this dataset
                # for updates
                self._rows.append(
                    MutableDatasetRow(
                        identifier=row.identifier,
                        values=row.values,
                        dataset=self
                    )
                )
        return self._rows

    def to_bokeh(self, columns = None):
        """Convert the dataset to a bokeh ColumnDataSource

        Parameters
        ----------
        columns: list(string or int) (optional)
            The columns to include.  (default: All columns)

        Returns
        -------
        bokeh.models.sources.ColumnDataSource
        """

        if columns is None:
            columns = self.columns
        return ColumnDataSource(dict([
            (
                column.name,
                [ row.get_value(column.identifier if column.identifier >= 0 else column.name) for row in self.rows ]
            )
            for column in self.columns
        ]))

    def show_map(self, lat_col, lon_col, label_col=None, center_lat=None, center_lon=None, zoom=8, height="500", map_provider='OSM'):
        import numpy as np # type: ignore[import]
        width="100%"
        addrpts = list()
        lats = []
        lons = []
        for row in self.rows:
            lon, lat = float(row.get_value(lon_col)), float(row.get_value(lat_col))
            lats.append(lat)
            lons.append(lon)
            if map_provider == 'Google':
                addrpts.append({"lat":str(lat), "lng":str(lon)})
            elif map_provider == 'OSM':
                label = ''
                if not label_col is None:
                    label = str(row.get_value(label_col))
                rowstr = '[' + str(lat) + ', ' + \
                             str(lon) + ', \'' + \
                             label + '\']'
                addrpts.append(rowstr)

        if center_lat is None:
            center_lat = np.mean(lats)

        if center_lon is None:
            center_lon = np.mean(lons)

        if map_provider == 'Google':
            import json
            from vizier.engine.packages.pycell.packages.wrappers import GoogleMapClusterWrapper
            html = GoogleMapClusterWrapper().do_output(json.dumps(addrpts), center_lat, center_lon, zoom, width, height) 
            self.client.show_html(html)
        elif map_provider == 'OSM':  
            from vizier.engine.packages.pycell.packages.wrappers import LeafletClusterWrapper
            html = LeafletClusterWrapper().do_output(addrpts, center_lat, center_lon, zoom, width, height) 
            self.client.show_html(html)
        else:
            print("Unknown map provider: please specify: OSM or Google")

    def show_d3_plot(self, chart_type, keys=list(), labels=list(), labels_inner=list(), value_cols=list(), key_col='KEY', width=600, height=400, title='', subtitle='', legend_title='Legend', x_cols=list(), y_cols=list(), date_cols=list(), open_cols=list(), high_cols=list(), low_cols=list(), close_cols=list(), volume_cols=list(), key=None):
        from vizier.engine.packages.pycell.packages.wrappers import D3ChartWrapper

        charttypes = ["table", "bar", "bar_stacked", "bar_horizontal", "bar_circular", "bar_cluster", "donut", 
                     "polar", "heat_rad", "heat_table", "punch", "bubble", "candle", "line", "radar", "rose"]
        
        if chart_type not in charttypes:
            print(("Please specify a valid chart type: one of: " + str(charttypes)))
            return

        if not labels:
            labels = keys

        if not labels_inner:
            labels_inner = value_cols

        data = []
        for key_idx, label in enumerate(labels):
            entry = {}
            entry['key'] = label
            entry['values'] = []
            for idx, label_inner in enumerate(labels_inner):
                inner_entry = {}
                inner_entry['key'] = label_inner
                for row in self.rows:
                    if len(keys) == 0 or (len(keys) >= key_idx and row.get_value(key_col) == keys[key_idx]):
                        if value_cols and len(value_cols) >= idx:
                            inner_entry['value'] = row.get_value(value_cols[idx])
                        if x_cols and len(x_cols) >= idx:
                            inner_entry['x'] = row.get_value(x_cols[idx])
                        if y_cols and len(y_cols) >= idx:
                            inner_entry['y'] = row.get_value(y_cols[idx])
                        if date_cols and len(date_cols) >= idx:
                            inner_entry['date'] = row.get_value(date_cols[idx])
                        if open_cols and len(open_cols) >= idx:
                            inner_entry['open'] = row.get_value(open_cols[idx])
                        if high_cols and len(high_cols) >= idx:
                            inner_entry['high'] = row.get_value(high_cols[idx])
                        if low_cols and len(low_cols) >= idx:
                            inner_entry['low'] = row.get_value(low_cols[idx])
                        if close_cols and len(close_cols) >= idx:
                            inner_entry['close'] = row.get_value(close_cols[idx])
                        if volume_cols and len(volume_cols) >= idx:
                            inner_entry['volume'] = row.get_value(volume_cols[idx])
                entry['values'].append(inner_entry)
            data.append(entry)

        if key is not None:
            data=data[data.index(key)]    
            
        html = D3ChartWrapper().do_output(data=data, charttype=chart_type, width=str(width), height=str(height), 
            title=title, subtitle=subtitle, legendtitle=legend_title)
        self.client.show_html(html)

    def show(self):
        self.client.show(self)
        
class MutableDatasetRow(DatasetRow):
    """Row in a Vizier DB dataset.

    Attributes
    ----------
    identifier: int
        Unique row identifier
    values : list(string)
        List of column values in the row
    """
    def __init__(self, 
        identifier: Optional[str] = None, 
        values: Optional[List[Any]] = None, 
        dataset: Optional[DatasetClient] = None
        ):
        """Initialize the row object.

        Parameters
        ----------
        identifier: int, optional
            Unique row identifier
        values : list(string), optional
            List of column values in the row
        dataset : vizier.datastore.client.DatasetClient, optional
            Reference to dataset that contains the row
        """
        identifier = "-1" if identifier is None else identifier
        super(MutableDatasetRow, self).__init__(
            identifier=identifier,
            values=values
        )
        self.dataset = dataset

    def __getitem__(self, key):
        return self.get_value(key)

    def annotations(self, column):
        """Get annotation object for given row cell.

        Parameters
        ----------
        column : int or string
            Column index, name, or label

        Returns
        -------
        vizier.engine.packages.pycell.client.ObjectMetadataSet
        """
        col_index = self.dataset.column_index(column)
        column_id = self.dataset.columns[col_index].identifier
        return ObjectMetadataSet(
            annotations=self.dataset.annotations.for_cell(
                column_id=column_id,
                row_id=self.identifier
            )
        )

    def get_value(self, column):
        """Get the row value for the given column.

        Parameters
        ----------
        column : int or string
            Column index, name, or label

        Returns
        -------
        string
        """
        col_index = self.dataset.column_index(column)
        return self.values[col_index]

    def set_value(self, column, value, clear_annotations=False):
        """Set the row value for the given column.

        Parameters
        ----------
        column : int or string
            Column index, name, or label
        value : string
            New cell value
        keep_annotations: bool, optional
            Flag indicating whether to keep or clear the annotations that are
            associated with this cell
        """
        col_index = self.dataset.column_index(column)
        self.values[col_index] = value
        if clear_annotations:
            self.dataset.annotations.clear_cell(
                column_id=self.dataset.columns[col_index].identifier,
                row_id=self.identifier
            )


class ObjectMetadataSet(object):
    """Query annotations for a dataset resource."""
    def __init__(self, annotations):
        """initialize the list of resource annotations.

        Parameters
        ----------
        annotations: list(vizier.datastore.annotation.base.DatasetAnnotation)
            List of resource annotations
        """
        self.annotations = annotations

    def contains(self, key):
        """Test if an annotation with given key exists for the resource.

        Parameters
        ----------
        key: string
            Annotation key

        Returns
        -------
        bool
        """
        return not self.find_one(key) is None

    def count(self):
        """Number of annotations for this resource.

        Returns
        -------
        int
        """
        return len(self.annotations)

    def find_all(self, key):
        """Get a list with all annotations that have a given key. Returns an
        empty list if no annotation with the given key exists.

        Parameters
        ----------
        key: string
            Key value for new annotation

        Returns
        -------
        list(vizier.datastore.annotation.base.DatasetAnnotation)
        """
        result = list()
        for anno in self.annotations:
            if anno.key == key:
                result.append(anno)
        return result

    def find_one(self, key):
        """Find the first annotation with given key. Returns None if no
        annotation with the given key exists.

        Parameters
        ----------
        key: string
            Key value for new annotation

        Returns
        -------
        vizier.datastore.annotation.base.DatasetAnnotation
        """
        for anno in self.annotations:
            if anno.key == key:
                return anno

    def keys(self):
        """List of existing annotation keys for the object.

        Returns
        -------
        list(string)
        """
        result = set()
        for anno in self.annotations:
            result.add(anno.key)
        return list(result)
