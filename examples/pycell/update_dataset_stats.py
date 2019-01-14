ds = vizierdb.get_dataset('stats')
for row in ds.rows:
    cat = row.get_value('grade_or_service_category_')
    if isinstance(cat, int):
        row.set_value('grade_or_service_category_', 'CAT ' + str(cat))
vizierdb.update_dataset(name='stats', dataset=ds)
