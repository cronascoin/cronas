#Copyright 2024 cronas.org
# rpc.py

import time
from aiohttp import web
import json
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class RPCServer:
    def __init__(self, peer, host, rpc_port, rpc_password):
        self.peer = peer
        self.host = host
        self.rpc_port = rpc_port
        self.rpc_password = rpc_password
        self.app = web.Application(middlewares=[self.auth_middleware])
        self.runner = None

    async def auth_middleware(self, app, handler):
        async def middleware_handler(request):
            if request.method == 'POST' and not self.check_auth(request):
                return web.Response(status=401, text='Unauthorized')
            return await handler(request)
        return middleware_handler

    def check_auth(self, request):
        if auth_info := request.headers.get('Authorization'):
            provided_password = auth_info.split()[-1]  # Basic handling; improve as needed
            return provided_password == self.rpc_password
        return False

    async def start_rpc_server(self):
        self.app.add_routes([
            web.post('/addnode', self.add_node),  # Register the route for adding a node
            web.get('/peers', self.get_peers)     # Route to get active peers
        ])
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.rpc_port)
        await site.start()
        logging.info(f"RPC server started on {self.host}:{self.rpc_port}")

    async def close_rpc_server(self):
        if self.runner:
            await self.runner.cleanup()
            logging.info("RPC Server closed.")

    async def get_peers(self, request):
        try:
            logging.info("Fetching active peers...")
            active_peers_list = [
                {
                    "server_id": peer_details['server_id'],
                    "addr": peer_details.get('addr', 'unknown'),
                    "addrlocal": peer_details.get('addrlocal', 'unknown'),
                    "addrbind": peer_details.get('addrbind', 'unknown'),
                    "version": peer_details.get('version', 'unknown')  # Providing default if not present
                }
                for peer_details in self.peer.active_peers.values()
            ]
            logging.debug(f"Prepared peers list for JSON serialization: {active_peers_list}")
            json_response = json.dumps(active_peers_list)
            logging.debug(f"Serialized JSON response: {json_response}")
            return web.Response(text=json_response, content_type='application/json')
        except Exception as e:
            logging.error(f"Error fetching peers: {e}")
            return web.Response(status=500, text=json.dumps({"error": "Internal server error"}))

    async def add_node(self, request):
        try:
            data = await request.json()
            required_fields = ['addr', 'addrlocal', 'addrbind', 'server_id', 'version']
            if any(field not in data for field in required_fields):
                return web.Response(status=400, text=json.dumps({"error": "Invalid request"}))
            
            addr = data['addr']
            addrlocal = data['addrlocal']
            addrbind = data['addrbind']
            server_id = data['server_id']
            version = data['version']
            peer_info = f"{addr}"
            self.peer.peers[peer_info] = int(time.time())
            self.peer.active_peers[peer_info] = {
                'addr': addr,
                'addrlocal': addrlocal,
                'addrbind': addrbind,
                'server_id': server_id,
                'version': version
            }
            self.peer.mark_peer_changed()
            await self.peer.rewrite_peers_file()
            return web.Response(
                text=json.dumps({"message": "Node added successfully"}),
                content_type='application/json'
            )
        except json.JSONDecodeError:
            return web.Response(status=400, text=json.dumps({"error": "Invalid JSON"}))
        except Exception as e:
            logging.error(f"Error adding node: {e}")
            return web.Response(status=500, text=json.dumps({"error": "Internal server error"}))

# Additional methods as needed
