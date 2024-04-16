from aiohttp import web
import json
import logging

class RPCServer:
    def __init__(self, peer, rpc, rpc_port):
        self.peer = peer
        self.host = rpc
        self.rpc_port = rpc_port

    async def start_rpc_server(self):
        app = web.Application()
        app.add_routes([
            web.get('/peers', self.get_peers),
            web.post('/addnode', self.add_node)  # Register the route for adding a node
        ])
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.rpc_port)
        await site.start()
        logging.info(f"RPC server started on {self.host}:{self.rpc_port}")

    async def get_peers(self, request):
        peers_list = list(self.peer.peers)
        return web.Response(text=json.dumps(peers_list), content_type='application/json')

    async def add_node(self, request):
        # Example implementation of adding a node
        data = await request.json()  # Extract JSON data from the request
        ip = data.get('ip')
        if ip:
            # Assuming peer has an add_peer method to add a new node
            self.peer.add_peer(ip)
            return web.Response(text=json.dumps({"message": "Node added successfully"}), content_type='application/json')
        else:
            return web.Response(status=400, text=json.dumps({"error": "Invalid request"}), content_type='application/json')
