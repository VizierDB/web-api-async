Access and Manipulate Datasets from Python Code
===============================================

The Python cell provides access to the underlying data store. This makes it possible to create, read, update, and delete datasets using Python  code. To interact with the data store, the Python cell contains the pre-initialized variable ```vizierdb```.

Read Dataset
------------

You can access an existing dataset using the ```get_dataset(name)``` method. The result is a dataset object that provides access to the dataset columns and rows. Below is an example that reads an existing dataset *employee* and prints the column names and the value for attribute *age* in each of the dataset rows.

```
ds = vizierdb.get_dataset('people')

# Print name of dataset columns
for col in ds.columns:
    print col.name

# Print value for attribute age in
# each dataset row
for row in ds.rows:
    print str(row.get_value('age'))
```

Columns can be reference by their name (will raise ValueError if column names are not unique), their spreadsheet label (i.e., A, B, C, D, ...), or the column index. You can use the ```column_index(name)``` method the get the index of a dataset column by name or spreadsheet label.


```
ds = vizierdb.get_dataset('employee')
# Column by name
print ds.rows[0].get_value('age')
# Column by spreadsheet label
print ds.rows[0].get_value('B')
# Column by index position
print ds.rows[0].get_value(1)

# Get index for column with name 'age'
c_index = ds.column_index('age')
# Print unique identifier of the 'age' column
print ds.columns[c_index].identifier
```


Update Dataset
--------------

To update cells in an dataset use the ```set_value(column, value)``` method of a dataset row. The following example converts the names of all employees to upper case and updates the existing dataset *employee*.

```
ds = vizierdb.get_dataset('employee')

# Convert employee names to upper case
for row in ds.rows:
    name = row.get_value('name')
    row.set_value('name', name.upper())

# Update the existing dataset
vizierdb.update_dataset('employee', ds)
```

To add a column to the dataset use the ```insert_column(name, position)``` method. The position argument is optional. The following example inserts a new column *department* to dataset *employee* and sets the value for department to R&D in all dataset rows.

```
ds = vizierdb.get_dataset('employee')

# Add column department at index position 1
ds.insert_column('department', position=1)

# Set department to default for all employees
for row in ds.rows:
    row.set_value('department', 'R&D')

vizierdb.update_dataset('employee', ds)
```

The method to add a new row is ```insert_row(values, position)```. Both arguments are optional. The value (if provided) has to be a list of the same length than the number of columns in the dataset. The following examples adds two new rows to an existing dataset.

```
ds = vizierdb.get_dataset('employee')

# Add empty row at end of dataset. Set name to 'Paul'
row = ds.insert_row()
row.set_value('name', 'Paul')

# Add row at beginning of dataset. Provide values for
# each column. The length of the list has to match the
# number of columns in the dataset
ds.insert_row(values=['Zara', 'R&D', 33, 45.78], position=0)

vizierdb.update_dataset('employee', ds)
```

To delete a column from a dataset use the ```delete_column(name)``` method. The method expects the name, spreadsheet label or index position of a column. It will also update all dataset rows. The following example deletes column *age* from dataset *employee*.

```
ds = vizierdb.get_dataset('employee')

# Delete column with name 'age'
ds.delete_column('age')

vizierdb.update_dataset('employee', ds)
```

Dataset rows can be deleted by removing them from the list that maintains the dataset rows. The following example removes the first row from dataset *employee* and stores the result as *last-years-employee*.

```
ds = vizierdb.get_dataset('employee')

# Delete first row using Pythons del and the row index
del ds.rows[0]

vizierdb.create_dataset('last-years-employee', ds)
```


Create Dataset
--------------

Instead of updating the existing dataset *employee* we can also store the update result in a new dataset *clean_employee*. In general, we can use the ```create_dataset(name, dataset)``` method to create a new dataset in the data store.

```
ds = vizierdb.get_dataset('employee')

# Convert employee names to upper case
for row in ds.rows:
    name = row.get_value('name')
    row.set_value('name', name.upper())

# Create a new dataset from modified one
vizierdb.create_dataset('clean_employee', ds)
```
It is also possible to create a new dataset completely from scratch. Use the ```new_dataset()``` method to obtain an empty dataset object. The new object can then be manipulated as described before.

```
# Get new dataset object
ds = vizierdb.new_dataset()

# Insert two columns
ds.insert_column('Name')
ds.insert_column('Age')

# Insert one row
ds.insert_row(values=['Alice', 25])

# Store new dataset
vizierdb.create_dataset('employee_of_month', ds)
```

Note that you could also overwrite an existing dataset with the new dataset by using ```update_dataset()``` instead of ```create_dataset()``` in the above example.


Rename Dataset
--------------

To rename an existing dataset use the ```rename_dataset(old_name, new_name)``` method.

```
vizierdb.rename_dataset('clean_employee', 'my_employee')
```

Delete Dataset
--------------

To delete an existing dataset use the ```drop_dataset(name)``` method.

```
vizierdb.drop_dataset('my_employee')
```

Annotations
-----------

It is possible to access and manipulate dataset annotations from within the Python code. Note that for annotations, columns and rows are identified by their internal unique identifier and not their name or index position. Below is an example that manipulates and prints annotations for dataset cells. Changes to dataset annotations are persisted when the dataset is saved.

```
# Get object for dataset with given name.
ds = vizierdb.get_dataset('employee')

# Get object for 'age' column
col_age = ds.columns[ds.column_index('age')]

# Add annotation to 'age' value for all employees
for row in ds.rows:
    annos = ds.annotations.for_cell(col_age.identifier, row.identifier)
    if not annos.contains('value:currency'):
        annos.add('value:currency', 'US Dollar')

# Print all annotations for employees 'age' value
for row in ds.rows:
    annos = ds.annotations.for_cell(col_age.identifier, row.identifier)
    for key in annos.keys():
        for a in annos.find_all(key):
            print a.key + ' = ' + str(a.value)

# Persist changes by saving the dataset        
vizierdb.update_dataset('employee', ds)
```
