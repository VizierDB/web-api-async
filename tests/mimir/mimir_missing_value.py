import json
import os


import vistrails.packages.mimir.init as mimir

CSV_FILE = '../data/dataset_with_missing_values.csv'

include_uncertainty = True
include_reasons = True
make_input_certain = True
materialize = False

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))
sql = 'SELECT * FROM ' + table_name

type_ = 'MISSING_VALUE'
#params = ['\'AGE > 40\'']
#
lens_name = mimir._mimir.createLens(
    table_name,
    mimir._jvmhelper.to_scala_seq(['\'AGE\'']),
    type_,
    make_input_certain,
    materialize
)

#print lens_name
sql = 'SELECT * FROM ' + lens_name
rs = json.loads(mimir._mimir.vistrailsQueryMimirJson(sql, True, True))

print json.dumps(rs, indent=4, sort_keys=True)

buf = mimir._mimir.explainCell('SELECT AGE FROM ' + lens_name + ' WHERE NAME = \'Claudia\'', 0, '')

print buf
for i in range(buf.size()):
    print buf.array()[i]

mimir.finalize()


#SELECT CASE WHEN rowid = 0 THEN 'Sue' ELSE col0 END AS col0,col1,col2 FROM

#SELECT rowid,col0,CASE WHEN rowid = 1 THEN '25' ELSE col1 END AS col1,col2 FROM
