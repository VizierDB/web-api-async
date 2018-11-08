import os


import vistrails.packages.mimir.init as mimir

CSV_FILE = './data/dataset3.csv'

include_uncertainty = False
include_reasons = False
make_input_certain = True
materialize = False

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))

view_name = mimir._mimir.createView(table_name, '(SELECT tid,Name,Salary,Age FROM ' + table_name + ') UNION ALL (SELECT 4 as tid,\'\' AS Name, \'\' as salary, \'\' AS age)')
view_name = mimir._mimir.createView(view_name, '(SELECT tid,Name,Salary,Age FROM ' + view_name + ') UNION ALL (SELECT 5 as tid,\'\' AS Name, \'\' as salary, \'\' AS age)')
view_name = mimir._mimir.createView(view_name, 'SELECT tid,Name,Salary,Age FROM ' + view_name + ' WHERE tid <> 2')

sql = 'SELECT tid,Name,Salary,Age FROM ' + view_name
print sql
csvStrDet = mimir._mimir.vistrailsQueryMimir(sql, include_uncertainty, include_reasons)
print csvStrDet.csvStr()

print csvStrDet.schema().get('NAME')
print csvStrDet.schema().get('AGE')
print csvStrDet.schema().get('SALARY')
print csvStrDet.schema().get('salary')

#mimir.finalize()


#SELECT CASE WHEN rowid = 0 THEN 'Sue' ELSE col0 END AS col0,col1,col2 FROM

#SELECT rowid,col0,CASE WHEN rowid = 1 THEN '25' ELSE col1 END AS col1,col2 FROM
