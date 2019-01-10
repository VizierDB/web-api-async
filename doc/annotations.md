Annotations
===========

In general annotations could be assigned to columns, rows, or individual cell values. Each annotation is a key,value pair that has a unique identifier. the identifier is used to refer to the annotation (allowing multiple annotations with the same key of a resource) e.g., to delete or update the annotation key or value.

Annotations are maintained as dataset metadata. Access to annotations is via the datastore (get_dataset_annotations() and update_dataset_annotations()). Vizier (at this point) does not reason over the annotation data. It simply exposes it via the API (and passes updates to the datastore). Annotations, however, are accessible in the Python cell.

The vizier.datastore.metadata.DatasetMetadataObject contains three ...
