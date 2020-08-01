# Starting Elasticsearch and Kibana

## Modify system configuration

In order to run elasticsearch in development mode,
a couple of operating system limits need to be increased

- Run the following command as root to increase the limit on mmap counts: 
  - `sysctl -w vm.max_map_count=262144`
- Add the following line to `/etc/security/limits.conf` to increase the max number of file descriptors:
  - `user         -       nofile          65535`

## Running Elasticsearch

The script `start-es.sh` shows an example of how to start Elasticsearch from a `.tar.gz` package
using `screen` to run it in the background.
Note that you must either replace the package files with those in the `config` directory, or
set the `ES_PATH_CONF` environment variable to tell Elasticsearch to look for config files
in that directory.

## Running Kibana

Kibana can be started in a similar way - make sure to replace 
the file `config/kibana.yml` in the package with the version 
`kibana/kibana.yml` from this repository.

# Ingest

`ingest.py` is the script for ingesting data, and can be run by specifying the directory containing ingest config files. For example:

`python ingest.py ccr`

### Files in the ingest config directories:

* **`ingest_config.json`** - Contains the settings for ingesting. The options are documented at the top of `ingest.py` and should be mostly self-explanatory.
* **`index_settings.json`** - The Elasticsearch settings for the index, which get read and loaded by `ingest.py` if the index is recreated. This file also contains the custom text analyzers.
* **`mapping.json`** - The Elasticsearch mapping for the index, which gets read and loaded by `ingest.py` if the index is recreated. This file tells Elasticsearch how to index every field. If new fields are automatically generated during the ingest, this file will be written to with the updated mapping after the ingest.
* **`queries.log`** - An output file to which queries run by `query.py` get appended.
* **`bulk_errors.json`** - An output file which will contain all errors which occur during the bulk ingest.

# Query

The file `query.py` contains example Query DSL and SQL queries, along with demonstrations of how to run them using the Python API. The documentation should be self-explanatory.
