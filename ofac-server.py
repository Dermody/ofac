"""
MIT License

Copyright (c) 2016 Sparrow A.I., LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# Contact: Scott Condie (scott@sparrowai.com)

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
from configparser import SafeConfigParser



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
    # This uses python's configparser module.  See https://docs.python.org/3.5/library/configparser.html for the correct format for the config file.  All entries go under the heading [server].
    config = SafeConfigParser()
    config.read("ofac-server.cfg")
    cfg = config['server']
    tornado.options.parse_command_line()
    app = tornado.web.Application([(r"/", ReqHandler) ])
    # Enter your encryption keys below. 
    https_server = tornado.httpserver.HTTPServer(app, ssl_options={"certfile":cfg['certfile_location'], "keyfile": cfg['keyfile_location']})
    # Now perform some searches to make sure that this worked.  
    tornado.platform.asyncio.AsyncIOMainLoop().install()
    ioloop = asyncio.get_event_loop()
    https_server.listen(cfg['port'])
    ioloop.run_forever()
