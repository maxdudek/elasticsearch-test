#!/bin/bash -e
screen -d -m -S es bash -c 'ES_PATH_CONF=~/elasticsearch-test/config/ ~/es/elasticsearch-7.7.0/bin/elasticsearch'