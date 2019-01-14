ds = vizierdb.get_dataset('stats')
for row in ds.rows:
    print row.get_value('grade_or_service_category_')
