#  Name: ofac-server.py
#  Author: Scott Condie (scott@sparrowai.com)
#  Description: A server that scrubs names against the OFAC SDN and consolidated lists.


import asyncio
import tornado.ioloop
import tornado.web
import tornado.platform.asyncio
import tornado.httpserver
import tornado.escape
import tornado.options
import json
from elasticsearch_async import AsyncElasticsearch
from pprint import pprint



client = AsyncElasticsearch()


async def build_profile(ent_num):
    res = await client.search(index="ofac", body={"query":{"multi_match": {"query": ent_num, "fields": ['ent_num']}}})
    try:
        max_score = res['hits']['max_score']
    except KeyError:
        return {}
    raw = res['hits']['hits']
    out_dict = {'max_score': max_score}
    for ii in raw:
        out_dict.update(ii['_source'])
    return out_dict



async def get_search_results(query, fields):
    res = await client.search(index="ofac", body={"query":{"multi_match": {"query": query, "fields": fields}}})
    hits = res['hits']['hits']
    out = []
    ans = {}
    for ii in hits:
        en = ii['_source']['ent_num']
        ans = await build_profile(en)
    return ans


class ReqHandler(tornado.web.RequestHandler):

    async def get(self):
        res = await get_search_results("Georges")
        self.write("Hello world!\n")


    async def post(self):
        data = tornado.escape.json_decode(self.request.body)

        #d = json.loads(data.decode())
        if data['field'] == 'name':
            field = ['ent_name','alt_name','remarks','alt_remarks','comments']
        if data['field'] == "address":
            field = ["address", 'add_remarks', 'comments']

        result = await get_search_results(data['value'], field) 
        res = tornado.escape.json_encode(result)
        self.write(res)




if __name__ == "__main__":
    tornado.options.parse_command_line()
    app = tornado.web.Application([(r"/", ReqHandler) ])
    https_server = tornado.httpserver.HTTPServer(app, ssl_options={"certfile":"/etc/letsencrypt/live/stellar.network/fullchain.pem", "keyfile": "/etc/letsencrypt/live/stellar.network/privkey.pem"})
    # Now perform some searches to make sure that this worked.  
    tornado.platform.asyncio.AsyncIOMainLoop().install()
    ioloop = asyncio.get_event_loop()
    https_server.listen(8888)
    ioloop.run_forever()
