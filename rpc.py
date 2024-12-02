# rpc.py

import asyncio
import json
import logging
import base64
import time  # Imported time module
from datetime import datetime  # Imported datetime class

from aiohttp import web

logger = logging.getLogger(__name__)

class RPCServer:
    def __init__(self, peer, host, rpc_port, rpc_password, rpc_username='admin'):
        self.peer = peer
        self.host = host
        self.rpc_port = rpc_port
        self.rpc_password = rpc_password
        self.rpc_username = rpc_username
        self.app = web.Application(middlewares=[self.rate_limit_middleware, self.auth_middleware])
        self.runner = None
        self.rate_limiter = asyncio.Semaphore(100)  # Max 100 concurrent requests
        self.add_routes()

    def add_routes(self):
        self.app.router.add_post('/addnode', self.handle_addnode)
        self.app.router.add_get('/getpeerinfo', self.handle_getpeerinfo)
        self.app.router.add_post('/sendmessage', self.handle_sendmessage)
        self.app.router.add_get('/getmessages', self.handle_getmessages)
        self.app.router.add_post('/transaction', self.handle_transaction)
        self.app.router.add_get('/health', self.handle_health_check)
        self.app.router.add_get('/getwalletinfo', self.handle_getwalletinfo)
        self.app.router.add_post('/sync', self.sync_blockchain)

    async def rate_limit_middleware(self, request, handler):
        try:
            await self.rate_limiter.acquire()
            return await handler(request)
        finally:
            self.rate_limiter.release()

    async def auth_middleware(self, request, handler):
        if request.path == '/health':
            return await handler(request)  # Health check is public
        auth_header = request.headers.get('Authorization', None)
        if not auth_header:
            return web.Response(status=401, text="Unauthorized")
        try:
            auth_type, credentials = auth_header.split(' ', 1)
            if auth_type.lower() != 'basic':
                raise ValueError("Unsupported auth type")
            decoded = base64.b64decode(credentials).decode('utf-8')
            username, password = decoded.split(':', 1)
            if username != self.rpc_username or password != self.rpc_password:
                raise ValueError("Invalid credentials")
        except Exception as e:
            logger.warning(f"Authentication failed: {e}")
            return web.Response(status=401, text="Unauthorized")
        return await handler(request)

    async def handle_addnode(self, request):
        try:
            data = await request.json()
            addr = data.get('addr')
            port = data.get('listening_port')
            if not addr or not port:
                return web.json_response({"error": "Missing 'addr' or 'listening_port'."}, status=400)
            peer_id = f"{addr}:{port}"
            if peer_id in self.peer.known_peers or peer_id in self.peer.active_peers:
                return web.json_response({"message": "Peer already exists."}, status=409)
            self.peer.known_peers[peer_id] = int(time.time())
            asyncio.create_task(self.peer.connect_to_peer(addr, port))
            return web.json_response({"message": "Node connection initiated."}, status=200)
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            logger.error(f"Error handling addnode: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_getpeerinfo(self, request):
        try:
            peers_info = []
            for peer_id, peer_data in self.peer.active_peers.items():
                uptime_seconds = int(time.time() - peer_data['lastseen'])
                peers_info.append({
                    "server_id": peer_id,
                    "addr": peer_id,
                    "addrlocal": f"{self.peer.host}:{self.rpc_port}",
                    "addrbind": f"0.0.0.0:{self.rpc_port}",
                    "lastseen": datetime.utcfromtimestamp(peer_data['lastseen']).isoformat() + "Z",
                    "uptime_seconds": uptime_seconds,
                    "ping": "0.123"  # Placeholder for ping time
                })
            return web.json_response({"peers": peers_info}, status=200)
        except Exception as e:
            logger.error(f"Error fetching peer info: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_sendmessage(self, request):
        try:
            data = await request.json()
            recipient_id = data.get('recipient_id')
            content = data.get('content')
            if not content:
                return web.json_response({"error": "Missing 'content'."}, status=400)
            if recipient_id:
                # Direct message
                asyncio.create_task(self.peer.message.send_direct_message(recipient_id, content))
                return web.json_response({"message": "Message sent to specific peer."}, status=200)
            else:
                # Broadcast message
                asyncio.create_task(self.peer.message.broadcast_message(content))
                return web.json_response({"message": "Message broadcasted to all peers."}, status=200)
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_getmessages(self, request):
        try:
            messages = self.peer.message.get_received_messages()
            self.peer.message.clear_messages()
            return web.json_response({"messages": messages}, status=200)
        except Exception as e:
            logger.error(f"Error fetching messages: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_transaction(self, request):
        try:
            data = await request.json()
            receiver_address = data.get('receiver_address')
            amount = data.get('amount')
            if not receiver_address or amount is None:
                return web.json_response({"error": "Missing 'receiver_address' or 'amount'."}, status=400)
            txid = await self.peer.crypto.create_transaction(receiver_address, amount)
            # Broadcast the transaction to peers
            asyncio.create_task(self.peer.message.broadcast_transaction(txid))
            return web.json_response({"message": "Transaction submitted successfully.", "transaction_hash": txid}, status=200)
        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON"}, status=400)
        except ValueError as ve:
            logger.warning(f"Transaction validation failed: {ve}")
            return web.json_response({"error": str(ve)}, status=400)
        except Exception as e:
            logger.error(f"Error handling transaction: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def handle_health_check(self, request):
        return web.json_response({"status": "OK"}, status=200)

    async def handle_getwalletinfo(self, request):
        try:
            wallet_info = {
                "address": self.peer.crypto.address,
                "balance": self.calculate_balance()
            }
            return web.json_response(wallet_info, status=200)
        except Exception as e:
            logger.error(f"Error fetching wallet info: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    def calculate_balance(self):
        balance = 0.0
        for utxo in self.peer.crypto.utxo_pool:
            if utxo['address'] == self.peer.crypto.address:
                balance += utxo['amount']
        return balance

    async def sync_blockchain(self, request):
        try:
            asyncio.create_task(self.peer.request_synchronization())
            return web.json_response({"message": "Blockchain synchronization initiated."}, status=200)
        except Exception as e:
            logger.error(f"Error initiating synchronization: {e}")
            return web.json_response({"error": "Internal server error"}, status=500)

    async def start_rpc_server(self):
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, self.host, self.rpc_port)
        await site.start()
        self.runner = runner
        logger.info(f"RPC server started on {self.host}:{self.rpc_port}")

    async def stop_rpc_server(self):
        if self.runner:
            await self.runner.cleanup()
            logger.info("RPC server stopped.")
