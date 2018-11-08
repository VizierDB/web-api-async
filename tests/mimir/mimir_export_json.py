import json
import os
import sys

import vistrails.packages.mimir.init as mimir

args = sys.argv[1:]
if len(args) != 2:
    

mimir.initialize()

table_name = mimir._mimir.loadCSV(os.path.abspath(CSV_FILE))

sql = 'SELECT * FROM ' + table_name
rs = mimir._mimir.vistrailsQueryMimirJson(sql, False, False)



#schema = json.loads(mimir._mimir.getSchema(sql))
#print schema

mimir.finalize()
