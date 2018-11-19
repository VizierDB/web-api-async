# File System Driver

Implements repository that maintains viztrails, branches, workflows and modules as files on the file system

Directory structure

/viztrails
    index.tsv
    /viztrails/<identifier>
        .properties
        branches/<identifier>
            .prov
            .properties
            .versions
            workflows/
                <identifier>.json
        modules
            <identifier>.json
