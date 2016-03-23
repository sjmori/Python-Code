__author__ = 'smorris'

# retrieve output from Kissmetrics Report

import requests
import json

import time
import datetime


start_date = '01/12/2014'
end_date = '10/12/2014'
start_time =  time.mktime(datetime.datetime.strptime(start_date, "%d/%m/%Y").timetuple())
end_time =  time.mktime(datetime.datetime.strptime(end_date, "%d/%m/%Y").timetuple())





url = 'https://api.kissmetrics.com/query/reports/2d232330-62b1-0132-6f29-22000a9a8afc/run'

header = {'Authorization': 'xxxxxxxxxxxxxxxxxxxxxxx',
              'Content-Type': 'application/json','Accept':'application/json'}


query_params = {
    'report': {

     'start_date': start_time, 'end_date':end_time
  }}


r = requests.post\
    (url = url,
     headers=header
   ,
    data=json.dumps(query_params)
    )


binary = r.content
output = json.loads(binary)



print output['query_guid']


status_url = 'https://api.kissmetrics.com/query/queries/'+output['query_guid']+'/status'

while True:

    r_status = requests.get\
    (url = status_url,
     headers=header

    )

    status_binary = r_status.content
    status_output = json.loads(status_binary)

    print status_output
    if status_output['completed'] == True\
            :  break


results_url = 'https://api.kissmetrics.com/query/queries/'+output['query_guid']+'/results'

r_results = requests.get\
(url = results_url,
 headers=header

)

results_binary = r_results.content
results_output = json.loads(results_binary)

print results_output










