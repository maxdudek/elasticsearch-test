from elasticsearch import Elasticsearch, helpers
from time import time, strftime
import json

INDEX = 'jobs-index'

LOG_FILE = 'queries.log'

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
        result = es.transport.perform_request('POST', '/_xpack/sql', params={'format': 'json'}, body={'query': body})
    elif queryType == 'count':
        result = es.count(index=INDEX, body=body)
    elif queryType == 'search':
        # WARNING: will output entire documnets unless configured otherwise
        result = es.search(index=INDEX, body=body)
    timeTaken = time() - startTime

    # Output results to log file
    with open(LOG_FILE, 'a') as log:
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

def clearCache():
    result = es.transport.perform_request('POST', '/_cache/clear')
    printJson(result)


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

# Example Nested DSL Query
nestedQuery = {
    "query":  {
        "nested" : {
            "path" : "network.nested_list",
            "query" : {
                "bool" : {
                    "must" : [
                    { "match" : {"network.nested_list._key" : "lo"} },
                    { "range" : {"network.nested_list.out-bytes.avg" : {"gt" : 1000000}} }
                    ]
                }
            }
        }
    }
}

# Example SQL query
sqlExample = 'SELECT AVG(cpu.nodecpus.user.avg), COUNT(*) FROM "jobs-index" WHERE acct.ncpus = 16'

# SQL queries return 'null' if the requested field is not present in a doc
sqlExample2 = 'SELECT "infiniband.qib0:1.switch-out-bytes.avg" FROM "jobs-index" LIMIT 100'

sqlExample3 = 'SELECT acct.ncpus, AVG(cpu.nodecpus.user.avg), COUNT(*) FROM "jobs-index" GROUP BY acct.ncpus'

sqlExample4 = 'SELECT MIN(acct.start_time), MAX(acct.start_time) FROM "jobs-index"'

joeQuery = """
SELECT COUNT (*)
FROM "jobs-index"
WHERE 
acct.end_time >= '2019-01-01'::datetime AND
"cpu.effcpus.all" = 1 AND
acct.ncpus > 1 AND
cpu.effcpus.user.avg > 0.9 AND
acct.exit_status = 'COMPLETED' AND
DATE_DIFF('minutes', acct.start_time, acct.end_time) > 10 AND
"cpu.nodecpus.all.cnt" - "cpu.jobcpus.all.cnt" = 0
"""
sqlExampleQuote = 'SELECT AVG("cpu.nodecpus.user.avg"), COUNT(*) FROM "jobs-index" WHERE "acct.ncpus" = 16'



# SQL queries can only be run when queryType == 'sql'
clearCache()
query(joeQuery, queryType='sql')
# query(nestedQuery, queryType='count')


# Test Comment 2
