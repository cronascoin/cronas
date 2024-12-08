# message.py

import json
import logging
import time
import asyncio
import base64
import aiofiles
import hashlib
import gzip
from jsonschema import validate, ValidationError
from schemas import (
    blockchain_chunk_schema,
    getblockchain_schema,
    utxo_data_schema,
    resend_chunk_schema,
    error_schema,
    heartbeat_schema,
    heartbeat_ack_schema,
    hello_schema,
    ack_schema,
    transaction_schema,
    daily_block_request_schema,
    daily_block_response_schema,
    block_generation_request_schema,
    block_schema,
    revocation_schema
)
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)

class MessageHandler:
    def __init__(self, peer):
        """
        Initialize the MessageHandler with a reference to the Peer instance.
        """
        self.peer = peer
        self.seen_message_ids = {}  # Dictionary to track processed message IDs and their timestamps
        self.resend_attempts = {}   # Dictionary to track resend attempts per chunk
        self.max_resend_attempts = 3  # Maximum number of resend attempts per chunk
    
        # Define handlers mapping
        self.handlers = {
            "hello": self.handle_hello,
            "ack": self.handle_ack,
            "peerlist": self.handle_peer_list,
            "getblockchain": self.handle_getblockchain,
            "blockchain_chunk": self.handle_blockchain_chunk,
            "getutxo": self.handle_get_utxo,
            "utxo_data": self.handle_utxo_data,
            "resend_chunk": self.handle_resend_chunk,
            "error": self.handle_error,
            "heartbeat": self.handle_heartbeat,
            "heartbeat_ack": self.handle_heartbeat_ack,
            # Removed old broadcast_message and direct_message handlers
            # Introduce a 'chat' message type
            "chat": self.handle_chat,
            "transaction": self.handle_transaction,
            "daily_block_request": self.handle_daily_block_request,
            "daily_block_response": self.handle_daily_block_response,
            "block_generation_request": self.handle_block_generation_request,
            "block": self.handle_block,
            "revocation": self.handle_revocation,
            "default": self.handle_default  # Default handler for unknown types
        }

        # Define schemas mapping
        # Removed broadcast_message_schema and direct_message_schema
        self.schemas = {
            "hello": hello_schema,
            "ack": ack_schema,
            "blockchain_chunk": blockchain_chunk_schema,
            "getblockchain": getblockchain_schema,
            "utxo_data": utxo_data_schema,
            "resend_chunk": resend_chunk_schema,
            "error": error_schema,
            "heartbeat": heartbeat_schema,
            "heartbeat_ack": heartbeat_ack_schema,
            # chat messages are handled without a schema here, or add a schema if desired
            "transaction": transaction_schema,
            "daily_block_request": daily_block_request_schema,
            "daily_block_response": daily_block_response_schema,
            "block_generation_request": block_generation_request_schema,
            "block": block_schema,
            "revocation": revocation_schema,
        }

        # Initialize heartbeat task
        self.heartbeat_interval = 60  # Send heartbeat every 60 seconds
        self.heartbeat_task = asyncio.create_task(self.send_heartbeats())

    def serialize_message(self, message, include_signature=True):
        """
        Serialize the message deterministically for signing or verification.

        Args:
            message (dict): The message to serialize.
            include_signature (bool): Whether to include the signature field.

        Returns:
            bytes: The serialized message.
        """
        message_copy = message.copy()
        if not include_signature:
            message_copy.pop('signature', None)
        return json.dumps(message_copy, sort_keys=True).encode('utf-8')

    async def send_message(self, writer, message):
        """
        Send a JSON-encoded message to a peer.

        Args:
            writer (StreamWriter): The StreamWriter object for the connection.
            message (dict): The message to send.
        """
        try:
            message_data = json.dumps(message).encode('utf-8')
            writer.write(message_data + b'\n')
            await writer.drain()
            logger.debug(f"Sent message: {message}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")

    async def receive_message(self, reader, timeout=10):
        """
        Receive a JSON-encoded message from a peer with a timeout.

        Args:
            reader (StreamReader): The StreamReader object for the connection.
            timeout (int): The maximum time to wait for a message in seconds.

        Returns:
            dict: The received message.
        """
        try:
            data = await asyncio.wait_for(reader.readuntil(separator=b'\n'), timeout=timeout)
            message = json.loads(data.decode().strip())
            logger.debug(f"Received message: {message}")
            return message
        except asyncio.TimeoutError:
            logger.error("Timeout while waiting for a message.")
            raise
        except asyncio.IncompleteReadError as e:
            logger.error(f"IncompleteReadError: {e}")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"JSONDecodeError: {e}")
            raise
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            raise

    async def send_hello_message(self, writer):
        """
        Send a signed hello message to a peer.

        Args:
            writer (StreamWriter): The StreamWriter object for the connection.

        Returns:
            float: The timestamp when the message was sent.
        """
        hello_message = {
            'type': 'hello',
            'version': self.peer.version,  # Obtained from the Peer instance
            'server_id': self.peer.server_id,  # Unique server identifier
            'timestamp': time.time() + self.peer.ntp_offset,
            # Instead of a certificate, send the public key
            'public_key': self.peer.crypto.get_public_key_pem()
        }
        serialized_message = self.serialize_message(hello_message, include_signature=False)
        signature = self.peer.crypto.sign(serialized_message)
        hello_message['signature'] = base64.b64encode(signature).decode('utf-8')
        
        send_time = time.time()  # Record the send time
        await self.send_message(writer, hello_message)
        logger.info(f"Sent signed hello message: {hello_message}")

        # Update active_peers with send_time for ping calculation
        peer_id = f"{writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}"
        if peer_id in self.peer.active_peers:
            self.peer.active_peers[peer_id]['send_time'] = send_time

        return send_time

    async def send_ack_message(self, writer):
        """
        Send a signed acknowledgment message to a peer.
        """
        ack_message = {
            "type": "ack",
            "version": self.peer.version,
            "server_id": self.peer.server_id,
            "timestamp": time.time() + self.peer.ntp_offset
        }
        serialized_message = self.serialize_message(ack_message, include_signature=False)
        signature = self.peer.crypto.sign(serialized_message)
        ack_message['signature'] = base64.b64encode(signature).decode('utf-8')

        await self.send_message(writer, ack_message)
        logger.info(f"Sent signed ack message to peer with server_id: {self.peer.server_id}")

    async def process_message(self, message, reader, writer):
        """
        Process incoming messages by dispatching them to the appropriate handlers.
        """
        peer_info = writer.get_extra_info('peername')
        if self.peer.debug:
            logger.info(f"Received message from {peer_info}: {message}")

        message_type = message.get("type")
        if not message_type:
            logger.warning("Received message without a type.")
            await self.send_error_message(writer, "Message type missing.")
            return

        if schema := self.schemas.get(message_type):
            try:
                validate(instance=message, schema=schema)
            except ValidationError as ve:
                logger.warning(f"Validation failed for message type {message_type}: {ve.message}")
                await self.send_error_message(writer, f"Invalid message format: {ve.message}")
                return
        elif message_type not in ("chat", "default"):
            logger.warning(f"No schema defined for message type {message_type}. Proceeding without validation.")

        handler = self.handlers.get(message_type)
        if not handler:
            logger.warning(f"Unknown message type received: {message_type}")
            await self.send_error_message(writer, f"Unknown message type: {message_type}")
            return

        # Define message types that require signature verification
        signature_required_types = {
            "hello", "ack", "heartbeat", "heartbeat_ack", "chat",
            "transaction", "daily_block_request",
            "daily_block_response", "block_generation_request", "block", "revocation"
        }

        if message_type in signature_required_types and not await self.verify_message_signature(message, writer):
            return

        await handler(message, reader, writer)

    async def verify_message_signature(self, message, writer):
        """
        Verify the signature of a message that requires authentication.
        """
        signature_b64 = message.get("signature")
        if not signature_b64:
            logger.warning(f"Missing signature in {message['type']} message.")
            await self.send_error_message(writer, "Missing signature in message.")
            return False

        try:
            signature = base64.b64decode(signature_b64)
        except base64.binascii.Error as e:
            logger.warning(f"Invalid signature encoding: {e}")
            await self.send_error_message(writer, "Invalid signature encoding.")
            return False

        message_content = {k: v for k, v in message.items() if k != 'signature'}
        serialized_message = self.serialize_message(message_content, include_signature=False)

        server_id = message.get('server_id')
        if not server_id:
            logger.warning("Missing server_id in message.")
            await self.send_error_message(writer, "Missing server_id in message.")
            return False

        peer_public_key = self.peer.crypto.get_public_key(server_id)
        if not peer_public_key:
            logger.error(f"No public key found for server_id {server_id}")
            await self.send_error_message(writer, "Peer public key not found.")
            return False

        try:
            peer_public_key.verify(
                signature,
                serialized_message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            logger.debug(f"Valid signature for message type {message['type']} from {server_id}.")
            return True
        except Exception as e:
            logger.warning(f"Invalid signature for message type {message['type']} from {server_id}: {e}")
            await self.send_error_message(writer, "Invalid signature.")
            return False

    # Handler Methods
    async def handle_hello(self, message, reader, writer):
        """
        Handle incoming signed hello messages.
        """
        receive_time = time.time()
        peer_server_id = message['server_id']
        peer_version = message.get('version', 'unknown')
        peer_timestamp = message.get('timestamp', receive_time)
        peer_info = writer.get_extra_info('peername')
        peer_info_str = f"{peer_info[0]}:{peer_info[1]}"
        peer_public_key_pem = message.get('public_key')  # Extract the public key

        # Calculate ping time
        ping = receive_time - peer_timestamp

        # Check for existing connection
        if peer_server_id in self.peer.active_peers:
            logger.info(f"Already connected to server_id {peer_server_id}. Closing new incoming connection.")
            writer.close()
            await writer.wait_closed()
            return

        # Store the peer's public key
        if peer_public_key_pem:
            self.peer.crypto.add_peer_public_key(peer_server_id, peer_public_key_pem)
        else:
            logger.warning(f"No public key provided by peer {peer_server_id}. Cannot verify their messages.")

        # Send ack message
        await self.send_ack_message(writer)

        # Register the connection
        await self.peer.register_peer_connection(
            reader,
            writer,
            peer_info_str,
            peer_server_id,
            peer_info[0],
            peer_info[1],
            peer_info[1],
            peer_version,
            ping,
        )

    async def handle_ack(self, message, reader, writer):
        """
        Handle incoming signed ack messages.
        """
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        remote_version = message.get("version", "unknown")
        remote_server_id = message.get("server_id", "unknown")

        local_addr, local_port = writer.get_extra_info('sockname')
        bound_port = local_port

        receive_time = time.time()
        send_time = self.peer.active_peers.get(remote_server_id, {}).get('send_time', receive_time)
        ping = receive_time - send_time

        await self.peer.register_peer_connection(
            reader, writer, peer_info, remote_server_id, local_addr, local_port, bound_port, remote_version, ping
        )

    async def handle_peer_list(self, message, reader, writer):
        """
        Handle incoming peer list messages.
        """
        data = message.get("data", {})
        new_peers = data.get("peers", [])
        logger.info(f"Received peer list: {new_peers}")

        valid_new_peers = []
        invalid_peers = []

        for peer_info in new_peers:
            if not self.peer.is_valid_peer(peer_info):
                invalid_peers.append(peer_info)
                continue

            if peer_info == f"{self.peer.host}:{self.peer.port}":
                logger.info(f"Skipping self-peer: {peer_info}")
                continue

            valid_new_peers.append(peer_info)

        if self.peer.debug:
            for invalid_peer in invalid_peers:
                logger.warning(f"Invalid peer received: {invalid_peer}")

        if valid_new_peers:
            for peer in valid_new_peers:
                self.peer.known_peers.add(peer)
            await self.peer.schedule_rewrite()

        for peer_info in valid_new_peers:
            if peer_info in self.peer.active_peers:
                if self.peer.debug:
                    logger.info(f"Already connected to {peer_info}, skipping additional connection attempt.")
                continue

            host, port_str = peer_info.split(':')
            port = int(port_str)
            connect_task = asyncio.create_task(self.peer.connect_to_peer(host, port))
            self.peer.background_tasks.append(connect_task)

        self.peer.update_active_peers()

    async def handle_getblockchain(self, message, reader, writer):
        """
        Handle incoming requests for blockchain data.
        """
        # Implementation unchanged from original, except no TLS references
        data = message.get("data", {})
        start_byte = data.get("start_byte", 0)
        chunk_size = data.get("chunk_size", 1048576)

        try:
            async with aiofiles.open(self.peer.crypto.blockchain_file, 'rb') as f:
                await f.seek(start_byte)
                chunk = await f.read(chunk_size)
                is_last_chunk = len(chunk) < chunk_size

            compressed_chunk = gzip.compress(chunk)
            checksum = hashlib.sha256(compressed_chunk).hexdigest()
            response = {
                "type": "blockchain_chunk",
                "data": {
                    "chunk": base64.b64encode(compressed_chunk).decode('utf-8'),
                    "start_byte": start_byte,
                    "end_byte": start_byte + len(compressed_chunk),
                    "is_last_chunk": is_last_chunk,
                    "checksum": checksum
                }
            }

            serialized_response = self.serialize_message(response, include_signature=False)
            signature = self.peer.crypto.sign(serialized_response)
            response['signature'] = base64.b64encode(signature).decode('utf-8')

            await self.send_message(writer, response)
            logger.info(f"Sent signed blockchain chunk: {start_byte}-{start_byte + len(compressed_chunk)} with checksum {checksum}")
        except FileNotFoundError:
            logger.error("Blockchain file not found.")
            error_response = {
                "type": "error",
                "data": {
                    "message": "Blockchain file not found."
                }
            }
            serialized_error = self.serialize_message(error_response, include_signature=False)
            signature = self.peer.crypto.sign(serialized_error)
            error_response['signature'] = base64.b64encode(signature).decode('utf-8')
            await self.send_message(writer, error_response)

    async def handle_blockchain_chunk(self, message, reader, writer):
        """
        Handle incoming blockchain data chunks.
        """
        # Implementation unchanged from original, except no TLS references
        data = message.get("data", {})
        chunk_base64 = data.get("chunk", "")
        start_byte = data.get("start_byte", 0)
        end_byte = data.get("end_byte", 0)
        is_last_chunk = data.get("is_last_chunk", False)
        received_checksum = data.get("checksum", "")
        peer_id = f"{writer.get_extra_info('peername')[0]}:{writer.get_extra_info('peername')[1]}"

        try:
            compressed_chunk = base64.b64decode(chunk_base64)
            computed_checksum = hashlib.sha256(compressed_chunk).hexdigest()
            if computed_checksum != received_checksum:
                logger.error(f"Checksum mismatch for chunk {start_byte}-{end_byte} from {peer_id}.")
                await self.send_error_message(writer, "Checksum mismatch. Chunk corrupted.", resend=True, start_byte=start_byte, chunk_size=end_byte - start_byte)
                return

            try:
                chunk = gzip.decompress(compressed_chunk)
            except gzip.BadGzipFile as e:
                logger.error(f"Failed to decompress chunk {start_byte}-{end_byte}: {e}")
                await self.send_error_message(writer, "Failed to decompress chunk.", resend=True, start_byte=start_byte, chunk_size=end_byte - start_byte)
                return

            if (start_byte, end_byte) not in self.peer.chunk_cache:
                self.peer.chunk_cache[(start_byte, end_byte)] = {}
            self.peer.chunk_cache[(start_byte, end_byte)][peer_id] = chunk

            if len(self.peer.chunk_cache[(start_byte, end_byte)]) >= 2:
                chunks = list(self.peer.chunk_cache[(start_byte, end_byte)].values())
                chunk_counts = {}
                for c in chunks:
                    c_hash = hashlib.sha256(c).hexdigest()
                    chunk_counts[c_hash] = chunk_counts.get(c_hash, 0) + 1

                verified_chunk_hash = max(chunk_counts, key=chunk_counts.get)
                verified_chunks = [c for c in chunks if hashlib.sha256(c).hexdigest() == verified_chunk_hash]

                if len(verified_chunks) >= 2:
                    await self.save_chunk(start_byte, end_byte, verified_chunks[0], writer)
                    del self.peer.chunk_cache[(start_byte, end_byte)]
                    logger.info(f"Chunk {start_byte}-{end_byte} verified by majority and saved.")
                else:
                    logger.warning(f"Checksum discrepancies persist for chunk {start_byte}-{end_byte}. Requesting resend.")
                    await self.send_error_message(writer, "Checksum discrepancies. Requesting resend.", resend=True, start_byte=start_byte, chunk_size=end_byte - start_byte)
            else:
                logger.info(f"Awaiting more sources for chunk {start_byte}-{end_byte}.")

                if not is_last_chunk:
                    next_start = end_byte
                    request = {
                        "type": "getblockchain",
                        "data": {
                            "start_byte": next_start,
                            "chunk_size": 1048576
                        }
                    }
                    serialized_request = self.serialize_message(request, include_signature=False)
                    signature = self.peer.crypto.sign(serialized_request)
                    request['signature'] = base64.b64encode(signature).decode('utf-8')

                    await self.send_message(writer, request)
                    logger.info(f"Requested next blockchain chunk starting at byte {next_start}")
                else:
                    logger.info("Completed receiving blockchain data.")
                    await self.request_utxo(writer)
        except base64.binascii.Error as e:
            logger.error(f"Failed to decode blockchain chunk: {e}")
            await self.send_error_message(writer, "Invalid blockchain chunk received.")

    async def handle_get_utxo(self, message, reader, writer):
        """
        Handle incoming UTXO data requests.
        """
        try:
            async with aiofiles.open(self.peer.crypto.utxo_file, 'r') as f:
                utxos = json.loads(await f.read())
            response = {
                "type": "utxo_data",
                "data": {
                    "utxos": utxos
                }
            }
            serialized_response = self.serialize_message(response, include_signature=False)
            signature = self.peer.crypto.sign(serialized_response)
            response['signature'] = base64.b64encode(signature).decode('utf-8')

            await self.send_message(writer, response)
            logger.info("Sent signed UTXO data to peer.")
        except FileNotFoundError:
            logger.error("UTXO file not found.")
            error_response = {
                "type": "error",
                "data": {
                    "message": "UTXO file not found."
                }
            }
            serialized_error = self.serialize_message(error_response, include_signature=False)
            signature = self.peer.crypto.sign(serialized_error)
            error_response['signature'] = base64.b64encode(signature).decode('utf-8')

            await self.send_message(writer, error_response)

    async def handle_utxo_data(self, message, reader, writer):
        """
        Handle incoming UTXO data.
        """
        data = message.get("data", {})
        utxos = data.get("utxos", [])
        try:
            async with aiofiles.open(self.peer.crypto.utxo_file, 'w') as f:
                await f.write(json.dumps(utxos, indent=4))
            logger.info("Received and saved UTXO data from peer.")
        except Exception as e:
            logger.error(f"Failed to save UTXO data: {e}")
            await self.send_error_message(writer, "Failed to save UTXO data.")

    async def handle_resend_chunk(self, message, reader, writer):
        """
        Handle incoming resend_chunk messages.
        """
        data = message.get("data", {})
        start_byte = data.get("start_byte", 0)
        chunk_size = data.get("chunk_size", 1048576)
        logger.info(f"Received resend request for chunk starting at {start_byte} with size {chunk_size}")
        request = {
            "type": "getblockchain",
            "data": {
                "start_byte": start_byte,
                "chunk_size": chunk_size
            }
        }
        serialized_request = self.serialize_message(request, include_signature=False)
        signature = self.peer.crypto.sign(serialized_request)
        request['signature'] = base64.b64encode(signature).decode('utf-8')

        await self.send_message(writer, request)
        logger.info(f"Sent signed resend request for chunk starting at byte {start_byte} with size {chunk_size}")

    async def handle_error(self, message, reader, writer):
        """
        Handle incoming error messages.
        """
        data = message.get("data", {})
        error_message = data.get("message", "Unknown error.")
        logger.warning(f"Received error from peer: {error_message}")

    async def handle_heartbeat(self, message, reader, writer):
        """
        Handle incoming signed heartbeat messages.
        """
        data = message.get("data", {})
        peer_server_id = data.get("server_id")
        uptime = data.get("uptime", 0)
        timestamp = data.get("timestamp", time.time())

        logger.info(f"Received heartbeat from {peer_server_id} with uptime {uptime} and timestamp {timestamp}")

        if peer_server_id not in self.peer.heartbeat_records:
            self.peer.heartbeat_records[peer_server_id] = []
        self.peer.heartbeat_records[peer_server_id].append({
            "uptime": uptime,
            "timestamp": timestamp
        })

        self.peer.uptime = uptime

        heartbeat_ack = {
            "type": "heartbeat_ack",
            "data": {
                "server_id": self.peer.server_id,
                "timestamp": time.time() + self.peer.ntp_offset
            }
        }
        serialized_ack = self.serialize_message(heartbeat_ack, include_signature=False)
        signature = self.peer.crypto.sign(serialized_ack)
        heartbeat_ack['signature'] = base64.b64encode(signature).decode('utf-8')

        await self.send_message(writer, heartbeat_ack)
        logger.info(f"Sent signed heartbeat acknowledgment to {peer_server_id}")

    async def handle_heartbeat_ack(self, message, reader, writer):
        """
        Handle incoming signed heartbeat acknowledgment messages.
        """
        data = message.get("data", {})
        peer_server_id = data.get("server_id")
        timestamp = data.get("timestamp", time.time())

        logger.info(f"Received heartbeat acknowledgment from {peer_server_id} at {timestamp}: Verified=True")

        if peer_server_id in self.peer.active_peers:
            self.peer.active_peers[peer_server_id]['last_seen'] = int(timestamp)
            logger.debug(f"Updated last seen for {peer_server_id} to {timestamp}")
        else:
            logger.warning(f"Received heartbeat_ack from unknown peer {peer_server_id}")

    async def handle_chat(self, message, reader, writer):
        """
        Handle incoming chat messages.
        """
        data = message.get("data", {})
        # Forward the message data to the Peer method for handling chat logic
        await self.peer.handle_incoming_chat_message(data)

    async def handle_transaction(self, message, reader, writer):
        """
        Handle incoming transaction messages.
        """
        txid = message.get("data", {}).get("txid")
        if not txid:
            logger.warning("Received transaction without txid.")
            await self.send_error_message(writer, "Transaction ID missing.")
            return

        if txid in self.seen_message_ids:
            logger.info(f"Duplicate transaction {txid} received. Ignoring.")
            return

        self.seen_message_ids[txid] = time.time()  # Record the timestamp

        await self.peer.handle_incoming_transaction(message)

    async def handle_daily_block_request(self, message, reader, writer):
        """
        Handle incoming daily_block_request messages.
        """
        data = message.get("data", {})
        requesting_server_id = data.get("server_id")
        timestamp = data.get("timestamp", time.time())

        logger.info(f"Received daily_block_request from {requesting_server_id} at {timestamp}")

        is_verified = self.peer.verify_uptime(requesting_server_id)

        response = {
            "type": "daily_block_response",
            "data": {
                "server_id": self.peer.server_id,
                "verified": is_verified,
                "timestamp": time.time() + self.peer.ntp_offset
            }
        }

        serialized_response = self.serialize_message(response, include_signature=False)
        signature = self.peer.crypto.sign(serialized_response)
        response['signature'] = base64.b64encode(signature).decode('utf-8')

        await self.send_message(writer, response)
        logger.info(f"Sent daily_block_response to {requesting_server_id}: Verified={is_verified}")

    async def handle_daily_block_response(self, message, reader, writer):
        """
        Handle incoming daily_block_response messages.
        """
        data = message.get("data", {})
        server_id = data.get("server_id")
        verified = data.get("verified")
        timestamp = data.get("timestamp", time.time())

        logger.info(f"Received daily_block_response from {server_id} at {timestamp}: Verified={verified}")

        self.peer.record_block_verification(server_id, verified, timestamp)

        if self.peer.can_generate_block():
            await self.peer.generate_and_broadcast_block()

    async def handle_block_generation_request(self, message, reader, writer):
        """
        Handle incoming block_generation_request messages.
        """
        data = message.get("data", {})
        requesting_server_id = data.get("server_id")
        block_data = data.get("block_data")

        logger.info(f"Received block_generation_request from {requesting_server_id}")

        if self.peer.is_authorized_to_generate_block(requesting_server_id):
            block = self.peer.create_block(block_data)
            await self.peer.broadcast_block(block)
            logger.info(f"Block generated and broadcasted by {requesting_server_id}")
        else:
            logger.warning(f"Unauthorized block_generation_request from {requesting_server_id}")
            await self.send_error_message(writer, "Unauthorized to generate block.")

    async def handle_block(self, message, reader, writer):
        """
        Handle incoming block messages.
        """
        data = message.get("data", {})
        block_hash = data.get("block_hash")
        block_data = data.get("data")
        timestamp = data.get("timestamp")

        logger.info(f"Received block with hash {block_hash} from {block_data.get('generator')} at {timestamp}")

        if self.peer.verify_block(block_data):
            self.peer.crypto.blockchain.append(data)
            await self.peer.crypto._save_to_file(self.peer.crypto.blockchain_file, self.peer.crypto.blockchain)
            logger.info(f"Block {block_hash} added to the blockchain.")

            if block_hash not in self.peer.block_votes:
                self.peer.block_votes[block_hash] = 0
            self.peer.block_votes[block_hash] += 1

            if self.peer.block_votes[block_hash] >= self.peer.get_vote_threshold():
                await self.peer.reward_node(block_data.get("generator"))
        else:
            logger.warning(f"Invalid block received: {block_hash}")
            await self.send_error_message(writer, "Invalid block received.")

    async def handle_revocation(self, message, reader, writer):
        """
        Handle incoming revocation messages.
        """
        data = message.get("data", {})
        revoked_server_id = data.get("server_id")
        reason = data.get("reason", "No reason provided")
        timestamp = data.get("timestamp", time.time())

        if not revoked_server_id:
            logger.warning("Revocation message missing server_id.")
            await self.send_error_message(writer, "Revoked server_id missing.")
            return

        logger.info(f"Received revocation for server_id {revoked_server_id} at {timestamp}: Reason={reason}")

        authority_ids = self.peer.get_authority_ids()
        sender_server_id = message.get("server_id")
        if sender_server_id not in authority_ids:
            logger.warning(f"Revocation message from unauthorized server_id {sender_server_id}. Ignoring.")
            await self.send_error_message(writer, "Unauthorized to send revocation.")
            return

        if revoked_server_id in self.peer.active_peers:
            peer_info = self.peer.active_peers.pop(revoked_server_id)
            if (writer_to_close := peer_info.get('writer')):
                writer_to_close.close()
                await writer_to_close.wait_closed()
                logger.info(f"Closed connection with revoked peer {revoked_server_id}")

        if revoked_server_id in self.peer.known_peers:
            self.peer.known_peers.remove(revoked_server_id)
            logger.info(f"Removed revoked peer {revoked_server_id} from known peers")

        await self.peer.broadcast_message(message)
        logger.info(f"Broadcasted revocation of {revoked_server_id} to other peers")

    async def handle_default(self, message, reader, writer):
        """
        Handle unknown message types.
        """
        message_type = message.get("type", "unknown")
        logger.warning(f"Received unknown message type: {message_type}")
        await self.send_error_message(writer, f"Unknown message type: {message_type}")

    # Additional Methods

    async def save_chunk(self, start_byte, end_byte, chunk, writer):
        """
        Save the verified chunk to the blockchain file.
        """
        try:
            async with aiofiles.open(self.peer.crypto.blockchain_file, 'ab') as f:
                await f.seek(start_byte)
                await f.write(chunk)
            logger.info(f"Saved verified blockchain chunk: {start_byte}-{end_byte}")
        except Exception as e:
            logger.error(f"Failed to save blockchain chunk {start_byte}-{end_byte}: {e}")
            await self.send_error_message(writer, "Failed to save blockchain chunk.")

    async def send_error_message(self, writer, error_message, resend=False, start_byte=None, chunk_size=None):
        """
        Send a signed error message to the peer, optionally requesting a resend.
        """
        error_msg = {
            "type": "error",
            "data": {
                "message": error_message
            }
        }
        serialized_error = self.serialize_message(error_msg, include_signature=False)
        signature = self.peer.crypto.sign(serialized_error)
        error_msg['signature'] = base64.b64encode(signature).decode('utf-8')

        await self.send_message(writer, error_msg)
        logger.info(f"Sent signed error message to peer: {error_message}")

        if resend and start_byte is not None and chunk_size is not None:
            key = (start_byte, chunk_size)
            self.resend_attempts[key] = self.resend_attempts.get(key, 0) + 1
            if self.resend_attempts[key] > self.max_resend_attempts:
                logger.error(f"Max resend attempts reached for chunk starting at {start_byte}. Aborting resend.")
                return

            resend_request = {
                "type": "resend_chunk",
                "data": {
                    "start_byte": start_byte,
                    "chunk_size": chunk_size
                }
            }
            serialized_resend = self.serialize_message(resend_request, include_signature=False)
            resend_signature = self.peer.crypto.sign(serialized_resend)
            resend_request['signature'] = base64.b64encode(resend_signature).decode('utf-8')

            await self.send_message(writer, resend_request)
            logger.info(f"Sent signed resend request for chunk starting at byte {start_byte} with size {chunk_size}")

    async def send_heartbeats(self):
        """
        Periodically send signed heartbeat messages to all connected peers.
        """
        while not self.peer.shutdown_flag:
            await asyncio.sleep(self.heartbeat_interval)
            logger.debug("Sending heartbeat messages to all active peers.")
            for peer_id, peer_info in list(self.peer.active_peers.items()):
                writer = peer_info['writer']
                if writer.is_closing():
                    logger.warning(f"Cannot send heartbeat to {peer_id} as the connection is closing.")
                    continue
                heartbeat_message = {
                    "type": "heartbeat",
                    "data": {
                        "server_id": self.peer.server_id,
                        "timestamp": time.time() + self.peer.ntp_offset,
                        "uptime": self.peer.uptime
                    }
                }
                serialized_heartbeat = self.serialize_message(heartbeat_message, include_signature=False)
                signature = self.peer.crypto.sign(serialized_heartbeat)
                heartbeat_message['signature'] = base64.b64encode(signature).decode('utf-8')
                try:
                    await self.send_message(writer, heartbeat_message)
                    logger.debug(f"Sent signed heartbeat to {peer_id}")
                except Exception as e:
                    logger.error(f"Failed to send heartbeat to {peer_id}: {e}")
                    self.peer.mark_peer_unresponsive(peer_id)  # Implement this method in Peer
