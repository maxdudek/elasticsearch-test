from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers.errors import BulkIndexError
from bsonstream import KeyValueBSONInput
import gzip
import os
import json
import datetime
import time
import math

INDEX = 'jobs-index'

OP_TYPE = 'create'
# If _op_type == 'create', bulk ingest will not re-index documents already in the index
# If _op_type == 'index', all documents will be reindexed
# See https://elasticsearch-py.readthedocs.io/en/master/helpers.html

# A query body which matches all documents (used for counting total number of documents)
MATCH_ALL = { 'query': {'match_all': {}} }

# Path to gziped bson documents
DATA_PATH = '/data/documents'

def printJson(j):
    print(json.dumps(j, indent=4))

def writeJson(jsonObject, filename):
    with open(filename, 'w') as outFile:
        outFile.write(json.dumps(jsonObject, indent=4, separators=(',', ': ')))

def loadJson(filename):
    with open(filename, 'r') as inFile:
        return json.load(inFile)

# Load index mapping and settings from file
INDEX_MAPPING = loadJson('mapping.json')
INDEX_SETTINGS = loadJson('index_settings.json')
INDEX_BODY = {}
INDEX_BODY['mappings'] = INDEX_MAPPING['mappings']
INDEX_BODY['settings'] = INDEX_SETTINGS['settings']

# Map filenames to resource names
RESOURCE_NAMES = {
    'resource_8': 'chemistry',
    'resource_9': 'industry',
    'resource_10': 'mae',
    'resource_11': 'physics',
    'resource_13': 'ub-hpc',
    'resource_2909': 'faculty',
    'resource_14': 'alpha',
    'resource_15': 'bravo',
}

def transformDoc(doc, bulk=True):
    global OP_TYPE

    # Change key name to prevent naming conflict
    if '_id' in doc:
        doc['id'] = doc['_id']
        del doc['_id']
    
   
    if 'acct' in doc:
         # Turn list into object
        if 'hostcores' in doc['acct']:
            doc['acct']['hostcores'] = [{'hostname': h[0], 'value': h[1]} for h in doc['acct']['hostcores']]
            # Turn error text into int for consistency
            for h in doc['acct']['hostcores']:
                if h['value'] == ['error']:
                    h['value'] = [-1]
        # Timelimit should always be a number
        if 'timelimit' in doc['acct'] and isinstance(doc['acct']['timelimit'], basestring):
            if ':' in doc['acct']['timelimit']:
                doc['acct']['timelimit'] = timeToSeconds(doc['acct']['timelimit'])
            else:
                # TODO
                doc['acct']['timelimit'] = 0
    
    if bulk:
        doc['_index'] = INDEX
        
        doc['_op_type'] = OP_TYPE

    flattenErrors(doc)

    removeInvalidValues(doc)

    return doc

def timeToSeconds(timeString):
    """ Time is in form HH:mm:ss """
    try:
        x = time.strptime(timeString,'%H:%M:%S')
    except ValueError as e:
        return 0
    return int(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds())

def removeInvalidValues(v):
    """ Recursively remove invalid values like NaN and Infinity
        Returns true if the value should be deleted """
    if isinstance(v, dict):
        for key, value in v.items():
            if removeInvalidValues(value):
                del v[key]
        return False
    elif isinstance(v, list):
        for i in xrange(len(v) - 1, -1, -1):
            if removeInvalidValues(v[i]):
                del v[i]
        return False
    else:
        # Check if value is invalid
        if type(v) == float:
            return math.isnan(v) or math.isinf(v)
        return False

def flattenErrors(v):
    """ Recurrsively flatten fields labeled 'errors' into strings """
    if isinstance(v, dict):
        for key in v:
            flattenErrors(v[key])
        if 'errors' in v:
            v['errors'] = str(v['errors'])
    if isinstance(v, list):
        for item in v:
            flattenErrors(item)

def getFilesInDirectory(path, extension='bson.gz'):
    """ Recursively fetches a list of file paths with 
        the specified extension in a path 
    """
    files = []
    for r, d, f in os.walk(path):
        for filename in f:
            if filename[((len(extension)+1)*-1):] == ('.' + extension):
                files.append(os.path.join(r, filename))
    return files

def jsonIter(files, bulk=True):
    """ An iterator used for bulk ingest """
    for f in files:
        doc = transformDoc(loadJson(f), bulk=bulk)
        yield doc

def bsonIter(files, bulk=True):
    """ An iterator which opens a list of gziped, bson files
        files: a list of filenames 
    """
    for file in files:
        f = gzip.open(file, 'rb')
        stream = KeyValueBSONInput(fh=f)
        for doc in stream:
            doc = transformDoc(doc, bulk=bulk)
            if bulk:
                doc['_id'] = getDocId(doc, file)
            yield doc

def getDocId(doc, filename):
    """ Returns the unique identifier of a document based on: 
        1. resource name (from the filename)
        2. Job ID
        3. end time 
    """
    resourceName = RESOURCE_NAMES[filename.split('/')[-1].split('.')[0]]
    job_id = doc['acct']['id']
    endTime = str(doc['acct']['end_time'])
    return resourceName + '-' + job_id + '-' + endTime

def bulkIngest(es, dataPath):
    global OP_TYPE

    dataFiles = getFilesInDirectory(dataPath)
    print(dataFiles)
    errors = []

    # Bulk ingest
    print('\nBulk ingest...')
    count = 0
    startTime = time.time()
    # See https://elasticsearch-py.readthedocs.io/en/master/helpers.html 
    for _, error in helpers.streaming_bulk(es, bsonIter(dataFiles, bulk=True), yield_ok=False, raise_on_error=False):
        count += 1
        if 'version conflict' not in error[OP_TYPE]['error']['reason']: # Ignore duplicate ID error
            errors.append(error)

    timeTaken = time.time() - startTime
    print('Bulk ingest time: ' + str(timeTaken / 60.0) + ' minutes')
    writeJson(errors, 'bulk_errors.json')

def prep(es):
    """ Displays health info, DELETES AND RECREATES INDEX """
    global INDEX
    print('\nCluster health:')
    printJson(es.cluster.health())

    print('\nIndices:')
    print(es.cat.indices())

    # Delete index
    print('\nDeleting index ...')
    printJson(es.indices.delete(index=INDEX, ignore=[400, 404]))

    # Create index
    print('\nCreating index ...')
    printJson(es.indices.create(index=INDEX, body=INDEX_BODY, ignore=400))

es = Elasticsearch(hosts=['172.22.0.41'], timeout=60)           # Establish connection to elasticsearch

# prep(es)                                                        # Delete and recreate index
files = getFilesInDirectory(DATA_PATH)                          # Get the list of compressed bson files to ingest
bulkIngest(es, DATA_PATH)                                       # Ingest data

# Write mapping to file (Could change if autogenerated fields are added)
writeJson(es.indices.get_mapping(index=INDEX)[INDEX], 'mapping.json')
