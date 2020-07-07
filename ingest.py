from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers.errors import BulkIndexError
from bsonstream import KeyValueBSONInput # https://github.com/bauman/python-bson-streaming
import gzip
import os
import json
import time
import re
import math

# Important: in order to ensure that acct.resource_id is set correctly, files must 
# be named according to their resource - ex. 'resource_8.bson.gz'

HOSTNAME = '172.22.0.41'

# SWITCH based on which index you want to ingest into
# DIRECTORY = 'supremm'
DIRECTORY = 'tacc-stats'

def printJson(j):
    print(json.dumps(j, indent=4))

def writeJson(jsonObject, filename):
    with open(filename, 'w') as outFile:
        outFile.write(json.dumps(jsonObject, indent=4, separators=(',', ': ')))

def loadJson(filename):
    with open(filename, 'r') as inFile:
        return json.load(inFile)

INGEST_CONFIG = loadJson(DIRECTORY + '/' + 'ingest_config.json')

INDEX = INGEST_CONFIG['index_name']

# If _op_type == 'create', bulk ingest will not re-index documents already in the index
# If _op_type == 'index', all documents will be reindexed
# See https://elasticsearch-py.readthedocs.io/en/master/helpers.html
OP_TYPE = INGEST_CONFIG['op_type']

# Path to gziped bson documents
DATA_PATH = INGEST_CONFIG['data_path']

TRANSFORM_FUNCTION = INGEST_CONFIG['transform_function']

# See consolidateNestedFields()
NESTED_FIELDS = INGEST_CONFIG['nested_fields']

# See deleteFields() - these fields get removed from docs
FIELDS_TO_DELETE = INGEST_CONFIG['fields_to_delete']

# See flattenFields - these fields get converted to strings (in case they contain objects)
FIELDS_TO_FLATTEN = INGEST_CONFIG['fields_to_flatten']

# Map filenames to resource names
RESOURCE_NAMES = INGEST_CONFIG['resource_names']

# Object to be loaded with mapping and settings when index is created
INDEX_BODY = {}
# Configuration files
MAPPING_FILE = DIRECTORY + '/' + 'mapping.json'
SETTINGS_FILE = DIRECTORY + '/' + 'index_settings.json'

# A query body which matches all documents (used for counting total number of documents)
MATCH_ALL = { 'query': {'match_all': {}} }

count = 0

def transformDocSupremm(doc, filename, bulk=True):
    global OP_TYPE

    # Change key name to prevent naming conflict
    if '_id' in doc:
        doc['id'] = doc['_id']
        del doc['_id']

    if bulk:
        doc['_id'] = getDocId(doc, filename)
    
    if 'acct' in doc:
         # Turn list into object
        if 'hostcores' in doc['acct']:
            doc['acct']['hostcores'] = [{'hostname': h[0], 'value': h[1]} for h in doc['acct']['hostcores']]
            # Turn error text into int for consistency
            for h in doc['acct']['hostcores']:
                if h['value'] == ['error']:
                    h['value'] = [-1]
        # Timelimit should always be a number
        # TODO: when transitioning to Python 3, change 'basestring' --> 'str'
        if 'timelimit' in doc['acct'] and isinstance(doc['acct']['timelimit'], basestring):
            if ':' in doc['acct']['timelimit']:
                doc['acct']['timelimit'] = timeToSeconds(doc['acct']['timelimit'])
            else:
                doc['acct']['timelimit'] = 0
        # Ensure resource_id is set correctly, from the filename
        doc['acct']['resource_id'] = int(filename.split('/')[-1].split('.')[0].split('resource_')[-1])
        if 'reqmem' in doc['acct']:
            doc['acct']['reqmem'] = parseReqmem(doc['acct']['reqmem'])
   
    return doc

def transformDocTaccStats(doc, filename, bulk=True):

    # Change key name to prevent naming conflict
    if not bulk and '_id' in doc:
        doc['id'] = doc['_id']
        del doc['_id']

    return doc

# Universal transform function
def transformDoc(doc, filename, bulk=True):
    if bulk:
        doc['_index'] = INDEX
        doc['_op_type'] = OP_TYPE

    deleteFields(doc)
    removeInvalidValues(doc)
    consolidateNestedFields(doc)
    flattenFields(doc)    

    return doc

def deleteFields(doc):
    """
    Some fields are just too hard to work with
    So just delete them for now
    """
    for field in FIELDS_TO_DELETE:
        innerObject = doc

        subfields = field.split('.')

        # get inner object in document
        try:
            for subfield in subfields[:-1]:
                innerObject = innerObject[subfield]
            del innerObject[subfields[-1]]
        except KeyError:
            continue # If the doc doesn't contain the current field

def removeInvalidValues(v):
    """ Recursively remove invalid values like NaN and Infinity
        Returns true if the value should be deleted """
    if isinstance(v, dict):
        for key, value in v.items():
            if removeInvalidValues(value):
                del v[key]
        return False
    elif isinstance(v, list):
        for i in range(len(v) - 1, -1, -1):
            if removeInvalidValues(v[i]):
                del v[i]
        return False
    else:
        # Check if value is invalid
        if type(v) == float:
            return math.isnan(v) or math.isinf(v)
        return False

# For every field in NESTED_FIELDS, replaces the 
# contents of that field with a list instead of an object,
# to avoid a separate field being created for every object.
# The name of each object is mapped to a new '_key' field in
# each list element
def consolidateNestedFields(doc):
    for field in NESTED_FIELDS:
        innerObject = doc

        subfields = field.split('.')

        # get inner object in document
        try:
            for subfield in subfields:
                innerObject = innerObject[subfield]
        except KeyError:
            continue # If the doc doesn't contain the current field
        
        # Create a list based off of the elements of the inner object
        listToAdd = []
        for key in innerObject:
            value = innerObject[key]
            if 'error' not in key:
                if not isinstance(value, dict):
                    value = {"_value": value} # If the value is not an object, turn it into one
                value['_key'] = key
                listToAdd.append(value)
        
        # Delete old objects
        for element in listToAdd:
            del innerObject[element['_key']]
        
        # Replace the inner object with the list
        innerObject['nested_list'] = listToAdd

def parseReqmem(reqmem):
    """
    Converts a reqmem of 183000Mn into:
    {
        "megabytes": 183000,
        "type": "n"
    }
    Converts a reqmem of 600Gc into:
    {
        "megabytes": 614400,
        "type": "c"
    }
    """
    try:
        result = {}
        result['type'] = reqmem[-1]
        units = reqmem[-2]
        quantity = float(reqmem[:-2])
        if (units == 'K'):
            result['megabytes'] = int(round((quantity / 1024)))
        elif (units == 'M'):
            result['megabytes'] = int(round(quantity))
        elif (units == 'G'):
            result['megabytes'] = int(round(quantity * 1024))
        elif (units == 'T'):
            result['megabytes'] = int(round(quantity * 1024 * 1024))
    except Exception:
        return {'type': reqmem, 'megabytes': 0}
    return result

def timeToSeconds(timeString):
    result = re.match('^(?:([0-9]+)-)?([0-9]{2}):([0-9]{2}):([0-9]{2})$', timeString)
    if result == None:
        return 0
    matches = result.groups()
    if matches[0] == None:
        # HH:mm:ss
        matches = [int(m) for m in matches[1:]]
        return (matches[0]*3600) + (matches[1]*60) + matches[2]
    else:
        # D-HH:mm:ss
        matches = [int(m) for m in matches]
        return (matches[0]*24*3600) + (matches[1]*3600) + (matches[2]*60) + matches[3]

# Obsolete
# def timeToSeconds(timeString):
#     """ Time is in form HH:mm:ss or D-HH:mm:ss """
#     try:
#         x = time.strptime(timeString,'%H:%M:%S')
#     except ValueError as e:
#         return 0
#     return int(datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds())

# Obsolete - use flattenFields()
# ERRORS_TO_FLATTEN = ['errors', 'error']
# def flattenErrors(v):
#     """ Recurrsively flatten fields labeled 'errors' into strings """
#     if isinstance(v, dict):
#         for key in v:
#             flattenErrors(v[key])
#         for fieldname in ERRORS_TO_FLATTEN:
#             if fieldname in v:
#                 v[fieldname] = json.dumps(v[fieldname])
#     if isinstance(v, list):
#         for item in v:
#             flattenErrors(item)

def flattenFields(doc):
    """
    Convert the fields in FIELDS_TO_FLATTEN into strings (because sometimes they're objects)
    """
    for field in FIELDS_TO_FLATTEN:
        innerObject = doc

        subfields = field.split('.')

        # get inner object in document
        try:
            for subfield in subfields[:-1]:
                innerObject = innerObject[subfield]
            innerObject[subfields[-1]] = json.dumps(innerObject[subfields[-1]])
        except KeyError:
            continue # If the doc doesn't contain the current field

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

# Not used anymore - only for testing a small amount of files
def jsonIter(files, bulk=True):
    """ An iterator used for bulk ingest """
    for f in files:
        doc = transformDoc(loadJson(f), filename=f, bulk=bulk)
        yield doc

def bsonIter(files, bulk=True):
    """ An iterator which opens a list of gziped, bson files
        files: a list of filenames 
    """
    global count
    for file in files:
        f = gzip.open(file, 'rb')
        stream = KeyValueBSONInput(fh=f)
        print('Opening file ' + file)
        for doc in stream:
            doc = eval(TRANSFORM_FUNCTION + '(doc, filename=file, bulk=bulk)') # index-specific transform function
            doc = transformDoc(doc, filename=file, bulk=bulk)                 # universal transform function

            count += 1
            if count % 500 == 0:
                print('Ingested ' + str(count) + ' docs')
            
            yield doc

def getDocId(doc, filename):
    """ Returns the unique identifier of a document based on: 
        1. resource name (from file name)
        2. Job ID
        3. end time 
    """
    resourceName = RESOURCE_NAMES[filename.split('/')[-1].split('.')[0]]
    # resource_id = 'resource_' + str(doc['acct']['resource_id'])
    job_id = doc['acct']['id']
    endTime = str(doc['acct']['end_time'])
    return resourceName + '-' + job_id + '-' + endTime

def bulkIngest(es, dataPath):
    global OP_TYPE

    dataFiles = getFilesInDirectory(dataPath)
    print('Files to be ingested = ' + str(dataFiles))
    errors = []

    # Bulk ingest
    print('\nBeginning bulk ingest...')
    startTime = time.time()
    # See https://elasticsearch-py.readthedocs.io/en/master/helpers.html 
    for _, error in helpers.streaming_bulk(es, bsonIter(dataFiles, bulk=True), yield_ok=False, raise_on_error=False):
    # for _, error in helpers.parallel_bulk(es, bsonIter(dataFiles, bulk=True), raise_on_error=False):
        if 'version conflict' not in error[OP_TYPE]['error']['reason']: # Ignore duplicate ID error
            errors.append(error)
            if len(errors) % 500 == 0:
                print('Number of errors: ' + str(len(errors)))
                print('Current error reason: ' + json.dumps(error[OP_TYPE]['error']['reason'], indent=4))

    timeTaken = time.time() - startTime
    print('Bulk ingest time: ' + str(timeTaken / 3600.0) + ' hours.')
    writeJson(errors, DIRECTORY + '/' + 'bulk_errors.json')

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


if __name__ == '__main__':
    # Load index mapping and settings from file
    INDEX_MAPPING = loadJson(MAPPING_FILE)
    INDEX_SETTINGS = loadJson(SETTINGS_FILE)
    INDEX_BODY['mappings'] = INDEX_MAPPING['mappings']
    INDEX_BODY['settings'] = INDEX_SETTINGS['settings']

    es = Elasticsearch(hosts=[HOSTNAME], timeout=60)                # Establish connection to elasticsearch

    prep(es)                                                      # Delete and recreate index
    bulkIngest(es, DATA_PATH)                                       # Ingest data

    # Write mapping to file (Could change if autogenerated fields are added)
    writeJson(es.indices.get_mapping(index=INDEX)[INDEX], MAPPING_FILE)
