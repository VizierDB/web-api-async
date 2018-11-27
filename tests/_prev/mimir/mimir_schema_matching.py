import os


import vistrails.packages.mimir.init as mimir

CSV_FILE = '../data/dataset.csv'

include_uncertainty = True
include_reasons = True

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))

type_ = 'SCHEMA_MATCHING'
params = ["'A varchar'", "'B int'"]

lens_name = mimir._mimir.createAdaptiveSchema(
    table_name,
    mimir._jvmhelper.to_scala_seq(params),
    type_
)

print lens_name
sql = 'SELECT * FROM ' + lens_name
#print sql
csvStrDet = mimir._mimir.vistrailsQueryMimir(sql, True, True)
print csvStrDet.schema()
print csvStrDet.csvStr()
#print csvStrDet.colsDet()
#for c in csvStrDet.colsDet():
#    print c
#    print type(c)
#    for t in c:
#        print t
#print csvStrDet.rowsDet()
#for r in csvStrDet.rowsDet():
#    print r
reasons = csvStrDet.celReasons()
print 'NUMBER OF REASONS: ' + str(len(reasons))
for p in reasons:
    print 'REASON ' + str(p)
    for s in p:
        print '-> ' + s
    print '-'
#print csvStrDet.schema()
#print csvStrDet.schema().get('NAME')

mimir.finalize()


#SELECT CASE WHEN rowid = 0 THEN 'Sue' ELSE col0 END AS col0,col1,col2 FROM

#SELECT rowid,col0,CASE WHEN rowid = 1 THEN '25' ELSE col1 END AS col1,col2 FROM
