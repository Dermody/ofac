#  Name: ofac-server.py
#  Author: Scott Condie (scott@sparrowai.com)
#  Description: A server that scrubs names against the OFAC SDN and consolidated lists.


import asyncio
from ofacdb import *
import tornado.concurrent
import tornado.ioloop
import tornado.web
import tornado.platform.asyncio
import tornado.httpclient
from playhouse.shortcuts  import model_to_dict


from elasticsearch import Elasticsearch

es = Elasticsearch()

class ReqHandler(tornado.web.RequestHandler):
    async def get(self):
        print("request received")
        self.write("Hellow world!\n")



def add_to_es(dbclass):
    name = dbclass.__name__
    print("The name of this class is {}".format(name))
    res = dbclass.select()
    for ii in res:
        result = es.index(index="ofac", doc_type="SDN", body=model_to_dict(ii))



app = tornado.web.Application([(r"/", ReqHandler) ])

if __name__ == "__main__":

    # Query the database 
    #res = SDN.select().join(SDNAlternateIdentity, on=SDNAlternateIdentity.ent_num).where(SDN.ent_num == 36).get()
    #es.indices.delete(index=["ofac"])
    tables = [SDN, SDNAddress, SDNAlternateIdentity, SDNComment, Consolidated, ConsolidatedAddress, ConsolidatedAlternateIdentity, ConsolidatedComment]
    #for ii in tables:
    #    add_to_es(ii)





    # Now perform some searches to make sure that this worked.  
    res = es.search(index="ofac", body={"query": {"query_string": {"query": "Brazil"}}})
    print(res)
