import json
import os


import vistrails.packages.mimir.init as mimir

CSV_FILE = '../data/dataset_pick.csv'

include_uncertainty = True
include_reasons = True
make_input_certain = True
materialize = False

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))

type_ = 'PICKER'
params = ['PICK_FROM(AGE,SALARY)', 'HIDE_PICK_FROM(SALARY)']

lens_name = mimir._mimir.createLens(
    table_name,
    mimir._jvmhelper.to_scala_seq(params),
    type_,
    make_input_certain,
    materialize
)

print lens_name
sql = 'SELECT * FROM ' + lens_name
rs = json.loads(mimir._mimir.vistrailsQueryMimirJson(sql, True, True))

print json.dumps(rs['reasons'], indent=2, sort_keys=True)

mimir.finalize()


#SELECT CASE WHEN rowid = 0 THEN 'Sue' ELSE col0 END AS col0,col1,col2 FROM

#SELECT rowid,col0,CASE WHEN rowid = 1 THEN '25' ELSE col1 END AS col1,col2 FROM
