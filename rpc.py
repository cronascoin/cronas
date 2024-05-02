from aiohttp import web
import json
import logging

class RPCServer:
    def __init__(self, peer, host, rpc_port):
        self.peer = peer
        self.host = host
        self.rpc_port = rpc_port
        self.app = web.Application()
        self.runner = None

    async def start_rpc_server(self):
        app = web.Application()
        app.add_routes([
            web.post('/addnode', self.add_node),  # Register the route for adding a node
            web.get('/peers', self.get_peers)  # Route to get active peers
        ])
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.rpc_port)
        await site.start()
        logging.info(f"RPC server started on {self.host}:{self.rpc_port}")

    async def close_rpc_server(self):
        if self.runner:
            await self.runner.cleanup()
            logging.info("RPC Server closed.")

    async def get_peers(self, request):
        # Retrieve and format active peers
        active_peers_list = [f"{host}:{port}" for host, port in self.peer.active_peers.values()]
        return web.Response(text=json.dumps(active_peers_list), content_type='application/json')

    async def add_node(self, request):
        # Method for adding a new peer node
        data = await request.json()
        if ip := data.get('ip'):
            self.peer.add_peer(ip)
            return web.Response(text=json.dumps({"message": "Node added successfully"}), content_type='application/json')
        else:
            return web.Response(status=400, text=json.dumps({"error": "Invalid request"}), content_type='application/json')
