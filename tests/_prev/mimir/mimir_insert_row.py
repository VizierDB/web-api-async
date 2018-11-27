import json
import os
import vistrails.packages.mimir.init as mimir

#
# CSV__FILE - dataset.csv
#
#Name,Age,Salary
#Alice,23,35K
#Bob,32,30K

CSV_FILE = '../data/dataset.csv'

include_uncertainty = True
include_reasons = True

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))

view_name = mimir._mimir.createView(
    table_name,
    '(SELECT Name,Salary,Age FROM ' + table_name + ') ' +
    'UNION ALL ' +
    '(SELECT \'Claude\' AS Name, \'34K\' as Salary, Null AS Age)'
)

sql = 'SELECT Name,Salary,Age FROM ' + view_name
rs = json.loads(mimir._mimir.vistrailsQueryMimirJson(sql, include_uncertainty, include_reasons))
print json.dumps(rs, indent=4, sort_keys=True)

mimir.finalize()
