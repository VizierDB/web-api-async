import os


import vistrails.packages.mimir.init as mimir

CSV_FILE = './data/dataset3.csv'

include_uncertainty = False
include_reasons = False
make_input_certain = True
materialize = False

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))[:-4]

# ------------------------------------------------------------------------------
# Lense
# ------------------------------------------------------------------------------
def lens():
    type_ = 'MISSING_VALUE'
    params = 'AGE'

    res = mimir._mimir.createLens(
        table_name,
        mimir._jvmhelper.to_scala_seq(params),
        type_,
        make_input_certain,
        materialize
    )

def insert_delete():
    view_name = mimir._mimir.createView(table_name, '(SELECT tid,Name,Salary,Age FROM ' + table_name + ') UNION ALL (SELECT 4 as tid,\'\' AS Name, \'\' as salary, \'\' AS age)')
    view_name = mimir._mimir.createView(view_name, '(SELECT tid,Name,Salary,Age FROM ' + view_name + ') UNION ALL (SELECT 5 as tid,\'\' AS Name, \'\' as salary, \'\' AS age)')
    view_name = mimir._mimir.createView(view_name, 'SELECT tid,Name,Salary,Age FROM ' + view_name + ' WHERE tid <> 2')

#view_name = mimir._mimir.createView(table_name, 'SELECT Name,Salary,Age FROM ' + table_name)
#view_name = mimir._mimir.createView(table_name, 'SELECT Name,Salary,Age FROM ' + table_name + ' WHERE rowid() <> 1')
view_name = mimir._mimir.createView(table_name, '(SELECT Name,Salary,Age FROM ' + table_name + ') UNION ALL (SELECT \'\' AS Name, \'\' as salary, \'\' AS age)')
view_name = mimir._mimir.createView(view_name, '(SELECT Name,Salary,Age FROM ' + view_name + ') UNION ALL (SELECT \'\' AS Name, \'\' as salary, \'\' AS age)')
#csvStrDet = mimir._mimir.vistrailsQueryMimir('SELECT rowid,col0,col1,col2 FROM ' + view_name, include_uncertainty, include_reasons)
#print csvStrDet.csvStr()
#view_name = mimir._mimir.createView(table_name, 'SELECT rowid,col0,CASE WHEN rowid = 0 THEN 25 ELSE col1 END AS col1,col2 FROM ' + view_name)
sql = 'SELECT Name,Salary,Age FROM ' + view_name + ' WHERE ROWID() <> ROWID(1)'
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
