from elasticsearch import Elasticsearch, helpers
from time import time, strftime
import json

INDEX = 'jobs-index'

# Create a connection to the server
es = Elasticsearch(hosts=['172.22.0.41'], timeout=30)

def printJson(j):
    print(json.dumps(j, indent=4))

def query(body, queryType='sql'):
    """ Run a query and log the result in queries.log 
        body: the json body of the query
        queryType: 'sql', 'count', or 'search' """
    global es

    startTime = time()
    if queryType == 'sql':
        result = es.transport.perform_request('POST', '/_xpack/sql', params={'format': 'json'}, body=body)
    elif queryType == 'count':
        result = es.count(index=INDEX, body=body)
    elif queryType == 'search':
        # WARNING: will output entire documnets unless configured otherwise
        result = es.search(index=INDEX, body=body)
    timeTaken = time() - startTime

    # Output results to log file
    with open('queries.log', 'a') as log:
        logInfo = {}
        logInfo['time'] = strftime('%Y-%m-%d %H:%M:%S')
        logInfo['body'] = body
        logInfo['queryTime'] = timeTaken
        logInfo['result'] = result
        logInfo['type'] = queryType
        log.write('\n')
        log.write(json.dumps(logInfo, indent=4))
        log.write('\n')
        printJson(logInfo)


# Example Query DSL
dslExample = {
    "query": {
        "bool" : {
            "filter" : {
                "range": {
                    "acct.ncpus": {
                        "gte": 16,
                        "lte": 16
                    }
                }
            }
        }
    }
}

# Example SQL query
sqlExample = {
    "query": 'SELECT AVG(cpu.nodecpus.user.avg), COUNT(*) FROM "jobs-index" WHERE acct.ncpus = 16'
}

# SQL queries return 'null' if the requested field is not present in a doc
sqlExample2 = {
    "query": 'SELECT "infiniband.qib0:1.switch-out-bytes.avg" FROM "jobs-index" LIMIT 100'
}

# SQL queries can only be run when queryType == 'sql'
query(sqlExample, queryType='sql')
