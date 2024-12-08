# rpc.py

import asyncio
import json
import logging
import base64
from datetime import datetime
import os
import sys

from aiohttp import web

# Import the Peer and Crypto classes
from peer import Peer
from crypto import Crypto  # Ensure you have implemented the Crypto class
from message import MessageHandler  # Ensure message.py is correctly implemented

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG for detailed logs during development
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger(__name__)


class RPCServer:
    """
    RPCServer class to handle HTTP-based RPC requests for the Cronas P2P Network.
    """

    def __init__(
        self,
        peer: Peer,
        crypto: Crypto,
        host: str = '0.0.0.0',
        rpc_port: int = 4334,
        rpc_password: str = 'your_secure_password',
        rpc_username: str = 'admin'
    ):
        """
        Initialize the RPCServer instance.

        Args:
            peer (Peer): Instance of the Peer class for managing P2P connections.
            crypto (Crypto): Instance of the Crypto class for cryptographic operations.
            host (str, optional): Host address to bind the RPC server. Defaults to '0.0.0.0'.
            rpc_port (int, optional): Port number for the RPC server. Defaults to 4334.
            rpc_password (str, optional): Password for RPC authentication. Defaults to 'your_secure_password'.
            rpc_username (str, optional): Username for RPC authentication. Defaults to 'admin'.
        """
        self.peer = peer
        self.crypto = crypto
        self.host = host
        self.rpc_port = rpc_port
        self.rpc_password = rpc_password
        self.rpc_username = rpc_username

        # Initialize aiohttp web application with middlewares
        self.app = web.Application(middlewares=[self.rate_limit_middleware, self.auth_middleware])
        self.runner = None

        # Rate limiting: Max 100 concurrent requests
        self.rate_limiter = asyncio.Semaphore(100)

        # Add RPC routes
        self.add_routes()

    def add_routes(self):
        """
        Define and add all RPC routes to the aiohttp web application.
        """
        self.app.router.add_post('/addnode', self.handle_addnode)
        self.app.router.add_get('/getpeerinfo', self.handle_getpeerinfo)
        self.app.router.add_post('/sendmessage', self.handle_sendmessage)
        self.app.router.add_get('/getmessages', self.handle_getmessages)
        self.app.router.add_post('/transaction', self.handle_transaction)
        self.app.router.add_get('/health', self.handle_health_check)
        self.app.router.add_get('/getwalletinfo', self.handle_getwalletinfo)
        self.app.router.add_post('/sync', self.handle_sync_blockchain)

    async def rate_limit_middleware(self, request, handler):
        """
        Middleware to enforce rate limiting on incoming RPC requests.

        Args:
            request (web.Request): The incoming HTTP request.
            handler (Callable): The request handler.

        Returns:
            web.Response: The response from the handler.
        """
        try:
            await self.rate_limiter.acquire()
            return await handler(request)
        finally:
            self.rate_limiter.release()

    async def auth_middleware(self, request, handler):
        """
        Middleware to enforce Basic Authentication on RPC requests, excluding the health check.

        Args:
            request (web.Request): The incoming HTTP request.
            handler (Callable): The request handler.

        Returns:
            web.Response: The response from the handler or an unauthorized error.
        """
        if request.path == '/health':
            return await handler(request)  # Health check is public

        auth_header = request.headers.get('Authorization', None)
        if not auth_header:
            return web.Response(status=401, text="Unauthorized: Missing Authorization header.")

        try:
            auth_type, credentials = auth_header.split(' ', 1)
            if auth_type.lower() != 'basic':
                raise ValueError("Unsupported authentication type.")

            decoded_credentials = base64.b64decode(credentials).decode('utf-8')
            username, password = decoded_credentials.split(':', 1)

            if username != self.rpc_username or password != self.rpc_password:
                raise ValueError("Invalid username or password.")

        except Exception as e:
            logger.warning(f"Authentication failed: {e}")
            return web.Response(status=401, text="Unauthorized: Invalid credentials.")

        return await handler(request)

    async def handle_addnode(self, request):
        """
        Handle the 'addnode' RPC request to add a new peer to the network.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response indicating success or failure.
        """
        try:
            data = await request.json()
            addr = data.get('addr')
            port = data.get('listening_port')

            if not addr or not port:
                return web.json_response({"error": "Missing 'addr' or 'listening_port' fields."}, status=400)

            peer_id = f"{addr}:{port}"

            if peer_id in self.peer.known_peers or peer_id in self.peer.active_peers:
                return web.json_response({"message": "Peer already exists."}, status=409)

            # Add the new peer to known peers
            await self.peer.add_peer_to_known_peers(peer_id)

            # Initiate connection to the new peer
            asyncio.create_task(self.peer.connect_to_peer(addr, port))

            return web.json_response({"message": "Node connection initiated."}, status=200)

        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON format."}, status=400)
        except Exception as e:
            logger.error(f"Error handling addnode: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_getpeerinfo(self, request):
        """
        Handle the 'getpeerinfo' RPC request to retrieve information about connected peers.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response containing peer information.
        """
        try:
            peers_info = []
            current_time = time.time()
            for peer_id, peer_data in self.peer.active_peers.items():
                last_seen = peer_data.get('last_seen', current_time)
                uptime_seconds = int(current_time - peer_data.get('connected_at', current_time))
                ping_time = 0.123  # Placeholder for actual ping time; implement as needed

                peer_info = {
                    "server_id": peer_id,
                    "addr": peer_id,
                    "addrlocal": f"{self.peer.host}:{self.rpc_port}",
                    "addrbind": f"0.0.0.0:{self.rpc_port}",
                    "lastseen": datetime.utcfromtimestamp(last_seen).isoformat() + "Z",
                    "uptime_seconds": uptime_seconds,
                    "ping": ping_time
                }
                peers_info.append(peer_info)

            return web.json_response({"peers": peers_info}, status=200)

        except Exception as e:
            logger.error(f"Error fetching peer info: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_sendmessage(self, request):
        """
        Handle the 'sendmessage' RPC request to send a message to a specific peer or broadcast to all.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response indicating success or failure.
        """
        try:
            data = await request.json()
            recipient_id = data.get('recipient_id')
            content = data.get('content')

            if not content:
                return web.json_response({"error": "Missing 'content' field."}, status=400)

            if recipient_id:
                # Direct message
                asyncio.create_task(self.peer.message_handler.send_direct_message(recipient_id, content))
                return web.json_response({"message": "Message sent to specific peer."}, status=200)
            else:
                # Broadcast message
                asyncio.create_task(self.peer.message_handler.broadcast_message(content))
                return web.json_response({"message": "Message broadcasted to all peers."}, status=200)

        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON format."}, status=400)
        except Exception as e:
            logger.error(f"Error sending message: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_getmessages(self, request):
        """
        Handle the 'getmessages' RPC request to retrieve received messages.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response containing received messages.
        """
        try:
            messages = self.peer.message_handler.get_received_messages()
            self.peer.message_handler.clear_messages()
            return web.json_response({"messages": messages}, status=200)

        except Exception as e:
            logger.error(f"Error fetching messages: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_transaction(self, request):
        """
        Handle the 'transaction' RPC request to create and broadcast a transaction.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response containing transaction details.
        """
        try:
            data = await request.json()
            receiver_address = data.get('receiver_address')
            amount = data.get('amount')

            if not receiver_address or amount is None:
                return web.json_response({"error": "Missing 'receiver_address' or 'amount' fields."}, status=400)

            if not isinstance(amount, (int, float)) or amount <= 0:
                return web.json_response({"error": "'amount' must be a positive number."}, status=400)

            # Create the transaction using Crypto class
            txid = await self.crypto.create_transaction(receiver_address, amount)

            # Broadcast the transaction to all peers
            asyncio.create_task(self.peer.broadcast_transaction(txid))

            return web.json_response({"message": "Transaction submitted successfully.", "transaction_hash": txid}, status=200)

        except json.JSONDecodeError:
            return web.json_response({"error": "Invalid JSON format."}, status=400)
        except ValueError as ve:
            logger.warning(f"Transaction validation failed: {ve}")
            return web.json_response({"error": str(ve)}, status=400)
        except Exception as e:
            logger.error(f"Error handling transaction: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_health_check(self, request):
        """
        Handle the 'health' RPC request to perform a health check of the RPC server.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response indicating the server's health status.
        """
        try:
            return web.json_response({"status": "OK"}, status=200)
        except Exception as e:
            logger.error(f"Error performing health check: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_getwalletinfo(self, request):
        """
        Handle the 'getwalletinfo' RPC request to retrieve wallet address and balance.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response containing wallet information.
        """
        try:
            wallet_address = self.crypto.get_wallet_address()
            balance = self.crypto.calculate_balance()

            wallet_info = {
                "address": wallet_address,
                "balance": balance
            }

            return web.json_response(wallet_info, status=200)

        except Exception as e:
            logger.error(f"Error fetching wallet info: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def handle_sync_blockchain(self, request):
        """
        Handle the 'sync' RPC request to initiate blockchain synchronization.

        Args:
            request (web.Request): The incoming HTTP request.

        Returns:
            web.Response: JSON response indicating synchronization initiation.
        """
        try:
            asyncio.create_task(self.peer.request_synchronization())
            return web.json_response({"message": "Blockchain synchronization initiated."}, status=200)
        except Exception as e:
            logger.error(f"Error initiating synchronization: {e}")
            return web.json_response({"error": "Internal server error."}, status=500)

    async def start_rpc_server(self):
        """
        Start the aiohttp RPC server.
        """
        try:
            runner = web.AppRunner(self.app)
            await runner.setup()
            site = web.TCPSite(runner, self.host, self.rpc_port)
            await site.start()
            self.runner = runner
            logger.info(f"RPC server started on {self.host}:{self.rpc_port}")
        except Exception as e:
            logger.error(f"Failed to start RPC server: {e}")
            sys.exit(1)

    async def stop_rpc_server(self):
        """
        Stop the aiohttp RPC server gracefully.
        """
        if self.runner:
            await self.runner.cleanup()
            logger.info("RPC server stopped.")
