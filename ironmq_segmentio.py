__author__ = 'steve.morris'

####retrieve messages from Iron MQ sent by Segment.io and store in MongoDB

from iron_mq import *
import pprint

import pymongo
import json


db = pymongo.MongoClient('xxxxxxxxxxxxx',27017).segmentio


ironmq = IronMQ(host="xxxxxxxxx",project_id="xxxxxxxxxxxxxxxx",
                token="xxxxxxxxxxxxxxxxxxxxx",protocol="https", port=443,
                api_version=1,
                config_file=None)

pp = pprint.PrettyPrinter(indent=4)




queue = ironmq.queue("analytics")



while len(queue.get(max=1,timeout=0)["messages"])>0:

    msgs = queue.get(max=100)

    print len(msgs["messages"])

    #if len(msgs["messages"]) > 0:



    #6208063987588803165

    masterdata={}



    for msg in msgs["messages"]:


        msgid = msg["id"]

        masterdata['_id']=msgid
        msgbody = json.loads(msg["body"])
    #


        for msgdetail in msgbody['messages']:
            msgdetailjson={}
            msgdetaillist =[]
            msgdetailjson =  json.loads(msgdetail['body'])

            msgdetaillist.append(msgdetailjson)



        masterdata['body']=msgdetaillist
      #  pp.pprint(masterdata)


        db.Optimus.insert(masterdata)




        masterdata={}

       # print msgid

        returnstatus = queue.delete(msgid)


        print  str(msgid) + " " + str(returnstatus) + " Success!"

   # else:
      #  exit


