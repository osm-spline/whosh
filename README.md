## Whosh - a fast osm importer

This is whosh a striped down osm to postgres importer, which sould fit the needs of the xappy.js project.

# Dependencies
* zlib
* protobuf
* libxml2
* expat
* libpq

# How to build

    git submodlue init
    git submodlue update
    cd OSM-Binary/src
    make
    cd ../..
    make

