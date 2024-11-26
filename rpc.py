# rpc.py

import asyncio
import ipaddress
import time
from aiohttp import web
import json
import logging
import base64

logger = logging.getLogger(__name__)

class RPCServer:
    def __init__(self, peer, host, rpc_port, rpc_password, rpc_username='admin'):
        self.peer = peer
        self.host = host
        self.rpc_port = rpc_port
        self.rpc_password = rpc_password
        self.rpc_username = rpc_username
        self.app = web.Application(middlewares=[self.auth_middleware])
        self.runner = None

    @web.middleware
    async def auth_middleware(self, request, handler):
        if request.method in ['POST', 'GET'] and not self.check_auth(request):
            return web.Response(status=401, text='Unauthorized')
        return await handler(request)

    def check_auth(self, request):
        auth_header = request.headers.get('Authorization')
        if not auth_header:
            return False

        try:
            auth_type, credentials = auth_header.split(' ', 1)
            if auth_type.lower() != 'basic':
                return False
            decoded_credentials = base64.b64decode(credentials).decode('utf-8')
            username, password = decoded_credentials.split(':', 1)
            return username == self.rpc_username and password == self.rpc_password
        except (ValueError, base64.binascii.Error):
            return False

    async def start_rpc_server(self):
        self.app.add_routes([
            web.post('/addnode', self.add_node),
            web.get('/getpeerinfo', self.get_peer_info),
            web.post('/sendmessage', self.send_message),
            web.get('/getmessages', self.get_messages),
            web.post('/transaction', self.handle_transaction),
            web.get('/health', self.health_check),
            web.get('/getwalletinfo', self.get_wallet_info),  # Added endpoint
            # Add more routes as needed
        ])
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.rpc_port)
        await site.start()
        logger.info(f"RPC server started on {self.host}:{self.rpc_port}")

    async def close_rpc_server(self):
        if self.runner:
            logger.info("RPC server shutdown initiated.")
            await self.runner.cleanup()
            logger.info("RPC server shutdown complete.")

    async def get_peer_info(self, request):
        try:
            logging.info("Fetching peer information...")
            current_time = time.time()
            # Accessing uptime from block_reward if necessary
            if hasattr(self.peer, 'block_reward'):
                elapsed_time = current_time - self.peer.block_reward.last_update_time
                total_uptime = self.peer.block_reward.total_uptime + elapsed_time
            else:
                elapsed_time = current_time - self.peer.last_update_time
                total_uptime = self.peer.total_uptime + elapsed_time
            uptime_seconds = int(total_uptime)
            active_peers_list = [
                {
                    "server_id": peer_details.get('server_id', 'unknown'),
                    "version": peer_details.get('version', 'unknown'),
                    "addr": peer_details.get('addr', 'unknown'),
                    "addrlocal": peer_details.get('addrlocal', 'unknown'),
                    "addrbind": peer_details.get('addrbind', 'unknown'),
                    "lastseen": peer_details.get('lastseen', 'unknown'),
                    "ping": f"{peer_details.get('ping', 'unknown'):.3f}" if isinstance(peer_details.get('ping'), (int, float)) else 'unknown'
                }
                for peer_details in self.peer.active_peers.values()
            ]
            response = {
                "uptime": uptime_seconds,
                "connected_peers": active_peers_list
            }
            logging.debug(f"Prepared response for JSON serialization: {response}")
            return web.json_response(response, status=200)
        except Exception as e:
            logging.error(f"Error fetching peer information: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def add_node(self, request):
        try:
            data = await request.json()

            # More robust validation
            required_fields = ['addr', 'listening_port']
            if any(field not in data for field in required_fields):
                return web.json_response({"error": "Missing required fields"}, status=400)

            addr = data['addr']
            port = data['listening_port']

            # Input validation
            try:
                ipaddress.ip_address(addr)  # Validate IP address format
                port = int(port)
                if not (1 <= port <= 65535):
                    raise ValueError("Invalid port number")
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)

            peer_info = f"{addr}:{port}"

            # Check if peer already exists in known peers or active peers
            if peer_info in self.peer.peers or any(peer['addr'] == peer_info for peer in self.peer.active_peers.values()):
                return web.json_response({"error": "Peer already exists"}, status=409)

            current_time = int(time.time())
            self.peer.peers[peer_info] = current_time  # Add to known peers
            self.peer.peers_changed = True  # Mark peers as changed for persistence
            await self.peer.schedule_rewrite()

            # Trigger connection attempt
            asyncio.create_task(self.peer.connect_to_peer(addr, port))

            return web.json_response({"message": "Node connection initiated"}, status=200)
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            logging.error(f"Error adding node: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def send_message(self, request):
        try:
            data = await request.json()
            recipient_id = data.get('recipient_id')
            content = data.get('content')

            if not content:
                return web.json_response({"error": "Message content cannot be empty."}, status=400)

            if recipient_id:
                # Send a direct message
                success = await self.peer.send_message_to_peer(recipient_id, content)
                if success:
                    return web.json_response({"message": f"Message sent to {recipient_id}."}, status=200)
                else:
                    return web.json_response({"error": f"Failed to send message to {recipient_id}."}, status=500)
            else:
                # Broadcast the message
                await self.peer.broadcast_message(content)
                return web.json_response({"message": "Message broadcasted to all peers."}, status=200)
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            logging.error(f"Error sending message: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def get_messages(self, request):
        try:
            async with self.peer.message_lock:
                messages = list(self.peer.received_messages)  # Return a copy
                # Optionally, clear messages after fetching
                self.peer.received_messages.clear()

            return web.json_response({"messages": messages}, status=200)
        except Exception as e:
            logging.error(f"Error fetching messages: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_transaction(self, request):
        try:
            data = await request.json()
            receiver_address = data.get('receiver_address')
            amount = data.get('amount')

            # Basic validation
            if not all([receiver_address, amount]):
                return web.json_response({"error": "Missing required transaction fields."}, status=400)

            try:
                amount = float(amount)
                if amount <= 0:
                    raise ValueError("Amount must be positive.")
            except ValueError as e:
                return web.json_response({"error": str(e)}, status=400)

            # Create the transaction
            transaction = await self.peer.crypto.create_transaction(
                receiver_address,
                amount
            )

            if transaction:
                return web.json_response({
                    "message": "Transaction submitted successfully.",
                    "transaction_hash": transaction["hash"]
                }, status=200)
            else:
                return web.json_response({"error": "Failed to create transaction."}, status=500)
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            logging.error(f"Error handling transaction: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def health_check(self, request):
        return web.json_response({"status": "OK"}, status=200)

    async def get_wallet_info(self, request):
        """Endpoint to return the wallet address and balance."""
        try:
            wallet_info = self.peer.crypto.get_wallet_info()
            return web.json_response(wallet_info, status=200)
        except Exception as e:
            logging.error(f"Error fetching wallet info: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)
