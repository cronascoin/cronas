import asyncio
import ipaddress
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
            web.get('/getpeerinfo', self.get_peer_info)     # Route to get active peers
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

    async def get_peer_info(self, request):
        try:
            logging.info("Fetching peer information...")
            uptime = self.peer.get_uptime()
            active_peers_list = [
                {
                    "server_id": peer_details.get('server_id', 'unknown'),
                    "version": peer_details.get('version', 'unknown'),  # Providing default if not present
                    "addr": peer_details.get('addr', 'unknown'),
                    "addrlocal": peer_details.get('addrlocal', 'unknown'),
                    "addrbind": peer_details.get('addrbind', 'unknown'),
                    "lastseen": peer_details.get('lastseen', 'unknown'),
                    "ping": f"{peer_details.get('ping', 'unknown'):.3f}" if isinstance(peer_details.get('ping'), (int, float)) else 'unknown'  # Limit ping to 3 decimal places
                }
                for peer_details in self.peer.active_peers.values()
            ]
            response = {
                "uptime": uptime,
                "connected_peers": active_peers_list
            }
            logging.debug(f"Prepared response for JSON serialization: {response}")
            json_response = json.dumps(response)
            logging.debug(f"Serialized JSON response: {json_response}")
            return web.Response(text=json_response, content_type='application/json')
        except Exception as e:
            logging.error(f"Error fetching peer information: {e}")
            return web.Response(status=500, text=json.dumps({"error": "Internal server error"}))

    async def add_node(self, request):
        try:
            data = await request.json()

            # More robust validation
            required_fields = ['addr', 'listening_port']  # Adjusted required fields
            if not all(field in data for field in required_fields):
                return web.Response(status=400, text=json.dumps({"error": "Missing required fields"}))

            addr = data['addr']
            port = data['listening_port']  # Use listening_port directly

            # Input validation
            try:
                ipaddress.ip_address(addr)  # Validate IP address format
                if not isinstance(port, int) or not (1 <= port <= 65535):
                    raise ValueError("Invalid port number")
            except ValueError as e:
                return web.Response(status=400, text=json.dumps({"error": str(e)}))

            peer_info = f"{addr}:{port}"

            # Check if peer already exists
            if peer_info in self.peer.peers:
                return web.Response(status=409, text=json.dumps({"error": "Peer already exists"}))

            current_time = int(time.time())
            self.peer.peers[peer_info] = current_time  # Add to known peers

            # Trigger connection attempt (revised logic for directly adding to active_peers)
            asyncio.create_task(self.peer.connect_to_peer(addr, port))

            return web.Response(
                text=json.dumps({"message": "Node connection initiated"}),  # Updated message
                content_type='application/json'
            )
        except json.JSONDecodeError:
            return web.Response(status=400, text=json.dumps({"error": "Invalid JSON"}))
        except Exception as e:
            logging.error(f"Error adding node: {e}")
            return web.Response(status=500, text=json.dumps({"error": "Internal server error"}))

# Additional methods as needed
