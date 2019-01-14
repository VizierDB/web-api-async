ds = vizierdb.new_dataset()
ds.insert_column('Name')
ds.insert_column('Age')
ds.insert_row(['Alice', 23])
ds.insert_row(['Bob', 34])
ds = vizierdb.create_dataset('people', ds)
for row in ds.rows:
    print row.get_value('Name')
