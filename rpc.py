from aiohttp import web
import json
import logging

class RPCServer:
    def __init__(self, peer, host, rpc_port):
        self.peer = peer
        self.host = host
        self.rpc_port = rpc_port

    async def start_rpc_server(self):
        app = web.Application()
        app.add_routes([web.get('/peers', self.get_peers)])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.rpc_port)
        await site.start()
        logging.info(f"RPC server started on {self.host}:{self.rpc_port}")

    async def get_peers(self, request):
        peers_list = list(self.peer.peers)
        return web.Response(text=json.dumps(peers_list), content_type='application/json')
