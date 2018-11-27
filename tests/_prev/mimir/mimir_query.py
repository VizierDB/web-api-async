import json
import os
import vistrails.packages.mimir.init as mimir

#CSV_FILE = '../data/mimir/dataset.csv'
CSV_FILE = '../data/mimir/Employee.csv'

mimir.initialize()

filename = os.path.abspath(CSV_FILE)
print filename

table_name = mimir._mimir.loadCSV(filename, ',', True, True)

sql = 'SELECT * FROM ' + table_name
jstr = mimir._mimir.vistrailsQueryMimirJson(sql, True, True)
#for i in range(40):
#    pos = 49764478 + i
#    c = jstr[pos]
#    print str(pos) + '\t' + str(c) + '\t' + repr(c) + '\t' + str(ord(c))
#with open('JSONOUTPUTWIDE.json', 'w') as f:
#    f.write(jstr.encode('utf-8'))
rs = json.loads(jstr)

print json.dumps(rs, indent=4, sort_keys=True)

rs = mimir._mimir.explainCell(sql, 0, '2')
print rs
#print '\nSCHEMA\n'

#schema = json.loads(mimir._mimir.getSchema(sql))
#print schema

mimir.finalize()
