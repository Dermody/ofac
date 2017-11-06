# ofac

This library contains tools for scrubbing names against the OFAC's SDN and Consolidated lists (see [here](https://www.treasury.gov/resource-center/sanctions/Pages/default.aspx)) to aid in compliance of U.S. B.S./A.M.L. regulations.

## Requirements
* python >= 3.5
* peewee (Tested on 2.10.1)
* tornado
* elasticsearch_async
* A functioning local ElasticSearch server. (Tested on 2.4)

## License

This software is released under the terms of the MIT license. 

## Description

This project provides software that parses the SDN and Consolidated lists downloaded from the OFAC's website and then inserts the data from them into an Elasticsearch instance running locally. The software does not provide the locally running Elasticsearch instance.  

### Populating the elasticsearch database

`ofacdb.py` is a python3 script that parses the OFAC files (see the script for information on where such files need to be stored in order for the parser to find them).  It does this by creating a local sqlite database (`ofac.db`) that is then read into the elasticsearch instance.  

### Serving the results of a query

`ofac-server.py` provides a tornado-based webserver to receive POST requests representing database queries.  These requests should have as their body JSON of the form `{field: _field_name_, value: _the_value_}` where `_field_name_` is either `name` or `address` and `_the_value_` is the string to be searched for.  

The return value of these requests are JSON with all of the information (if any) related to the search term used.  

