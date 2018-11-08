import csv
import os
import shutil
import tempfile

from StringIO import StringIO

import vistrails.packages.mimir.init as mimir
from vizier.core.util import get_unique_identifier
from vizier.datastore.metadata import DatasetMetadata
from vizier.datastore.mimir import MimirDatasetColumn, MimirDatasetHandle
from vizier.datastore.mimir import COL_PREFIX, ROW_ID


def load_dataset(filename):
    """Create a table in Mimir from the given file.

    Parameters
    ----------
    filename: string
        Path to the file
    """
    # Create a copy of the original file under a unique name.
    tmp_file = get_tempfile()
    shutil.copyfile(filename, tmp_file)
    # Load dataset and retrieve the result to get the dataset schema
    init_load_name = mimir._mimir.loadCSV(tmp_file)
    sql = 'SELECT * FROM ' + init_load_name
    rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
    mimir_schema = rs.schema()
    reader = csv.reader(
        StringIO(rs.csvStr()),
        delimiter=',',
        skipinitialspace=True
    )
    # Write retieved result to temp file. Add unique column names and row
    # identifier
    os.remove(tmp_file)
    tmp_file = get_tempfile()
    # List of Mimir dataset column descriptors for the dataset schema
    columns = list()
    with open(tmp_file, 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_NONE)
        # Get dataset schema information from retrieved result
        out_schema = [ROW_ID.upper()]
        for name_in_dataset in reader.next():
            name_in_dataset = name_in_dataset.strip()
            col_id = len(columns)
            name_in_rdb = COL_PREFIX + str(col_id)
            out_schema.append(name_in_rdb)
            columns.append(
                MimirDatasetColumn(
                    col_id,
                    name_in_dataset,
                    name_in_rdb,
                    data_type=str(mimir_schema.get(name_in_dataset))[5:-1]
                )
            )
        writer.writerow(out_schema)
        # Remaining rows are dataset rows
        row_ids = list()
        for row in reader:
            row_id = len(row_ids)
            row_ids.append(row_id)
            out_row = [str(row_id)]
            for val in row:
                val = val.strip()
                if val.startswith('\'') and val.endswith('\''):
                    val = val[1:-1]
                elif val == 'NULL':
                    val = ''
                out_row.append(val)
            writer.writerow(out_row)
    table_name = mimir._mimir.loadCSV(tmp_file)
    os.remove(tmp_file)
    sql = 'SELECT * FROM ' + table_name
    rs = mimir._mimir.vistrailsQueryMimir(sql, True, True)
    reasons = rs.celReasons()
    uncertainty = rs.colsDet()
    return MimirDatasetHandle(
        get_unique_identifier(),
        columns,
        table_name,
        row_ids,
        len(columns),
        len(row_ids),
        annotations=get_annotations(columns, row_ids, reasons, uncertainty)
    )


def get_annotations(columns, row_ids, reasons, uncertainty, annotations=None):
    if annotations is None:
        annotations = DatasetMetadata()
    for row_index in range(len(row_ids)):
        if len(reasons) > row_index:
            comments = reasons[row_index]
            for col_index in range(len(columns)):
                anno = comments[col_index + 1]
                if anno != '':
                    annotations.for_cell(
                        columns[col_index].identifier,
                        row_ids[row_index]
                    ).set_annotation('mimir:reason', anno)
        if len(uncertainty) > row_index:
            for col_index in range(len(columns)):
                if uncertainty[row_index][col_index + 1] == False:
                    annotations.for_cell(
                        columns[col_index].identifier,
                        row_ids[row_index]
                    ).set_annotation('mimir:uncertain', 'true')
    return annotations


def get_tempfile():
    """Return the path to a temporary CSV file. Try to get a unique name to
    avoid problems with existing datasets.

    Returns
    -------
    string
    """
    tmp_prefix = 'DS_' + get_unique_identifier()
    return tempfile.mkstemp(suffix='.csv', prefix=tmp_prefix)[1]


CSV_FILE = './dataset_load_test.csv'
#CSV_FILE = '../data/dataset.csv'
#CSV_FILE = './reload_dataset.csv'

mimir.initialize()

ds = load_dataset(os.path.abspath(CSV_FILE))
print [col.name for col in ds.columns]
for row_id in ds.row_ids:
    for col in ds.columns:
        anno = ds.annotations.for_cell(col.identifier, row_id)

mimir.finalize()
