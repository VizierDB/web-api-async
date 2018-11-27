import os
import vistrails.packages.mimir.init as mimir

#
# CSV__FILE - dataset1.csv
#
#rid,col0,col1,col2
#0,Alice,23,35K
#1,Bob,32,30K

CSV_FILE = './data/dataset1.csv'

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))
print table_name

sql = "(SELECT rid,col0,col1,col2 FROM " + table_name + ") UNION ALL (SELECT 2 AS rid,'' AS col0,'' AS col1,'' AS col2)"
view_name = mimir._mimir.createView(table_name, sql)

sql = "SELECT rid,col0,col1,col2,'' AS col3 FROM " + view_name
view_name = mimir._mimir.createView(view_name, sql)

sql = "SELECT rid,col0,col1,col2,CASE WHEN rid = 0 THEN '180' ELSE col3 END AS col3 FROM " + view_name
view_name = mimir._mimir.createView(view_name, sql)

sql = "SELECT rid,col0,col1,col2,CASE WHEN rid = 2 THEN '160' ELSE col3 END AS col3 FROM " + view_name
view_name = mimir._mimir.createView(view_name, sql)

sql = "SELECT rid,col0,col1,col2,CASE WHEN rid = 1 THEN '170' ELSE col3 END AS col3 FROM " + view_name
view_name = mimir._mimir.createView(view_name, sql)

sql = "SELECT rid,CASE WHEN rid = 2 THEN 'Carla' ELSE col0 END AS col0,col1,col2,col3 FROM " + view_name
view_name = mimir._mimir.createView(view_name, sql)

sql = "SELECT rid,col0,CASE WHEN rid = 2 THEN 45 ELSE col1 END AS col1,col2,col3 FROM " + view_name
view_name = mimir._mimir.createView(view_name, sql)


sql = 'SELECT * FROM ' + view_name
csvStrDet = mimir._mimir.vistrailsQueryMimir(sql, True, True)

print 'VIEW ' + view_name
print csvStrDet.csvStr()

mimir.finalize()
