import asyncio
import json
import logging
import os
import socket
import uuid
import random

logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds if seeds else [])
        self.external_ip = self.detect_ip_address()
        self.load_peers()
        self.rewrite_peers_file()
        self.hello_seq = 0  # Initialize the hello_seq attribute here
        self.reconnect_delay = 5  # seconds
            
    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Connected to peer {addr}")
        try:
            data_buffer = ""
            while True:
                data = await reader.read(1024)
                if not data:
                    if reader.at_eof():
                        logging.info("Peer disconnected gracefully. Exiting listen loop.")
                        break
                    else:
                        logging.info("No data received. Waiting for more data...")
                        await asyncio.sleep(1)
                        continue
                
                data_buffer += data.decode()
                while '\n' in data_buffer:
                    message, data_buffer = data_buffer.split('\n', 1)
                    if message:
                        await self.process_message(json.loads(message), writer)

        except Exception as e:
            logging.error(f"Error during communication with {addr}: {e}")
        finally:
            logging.info(f"Closing connection with {addr}")
            writer.close()
            await writer.wait_closed()

    async def send_peer_list(self, writer):
        peer_list_message = {
            "type": "peer_list",
            "payload": list(self.peers),
            "server_id": self.server_id
        }
        writer.write(json.dumps(peer_list_message).encode() + b'\n')
        await writer.drain()
        logging.info("Sent peer list to a peer.")

    async def start_p2p_server(self):
        server = await asyncio.start_server(self.handle_peer_connection, self.host, self.p2p_port)
        logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
        async with server:
            await server.serve_forever()

    async def connect_to_peer(self, host, port, max_retries=5):
        if host in [self.host, self.external_ip, "127.0.0.1"]:
            logging.info("Attempted to connect to self. Skipping.")
            return

        attempt = 0
        writer = None

        try:
            while attempt < max_retries:
                logging.info(f"Attempt {attempt + 1} to connect to {host}:{port}")
                reader, writer = await asyncio.open_connection(host, port)
                
                self.hello_seq += 1
                handshake_msg = {
                    "type": "hello",
                    "payload": f"Hello from {self.host}",
                    "seq": self.hello_seq,
                    "server_id": self.server_id
                }
                writer.write(json.dumps(handshake_msg).encode() + b'\n')
                await writer.drain()

                data = await reader.readline()
                ack_message = json.loads(data.decode())
                if ack_message.get("type") == "ack":
                    logging.info(f"Handshake acknowledged by {host}:{port}")
                    logging.info("Successfully connected, starting to send heartbeats.")
                    asyncio.create_task(self.send_heartbeat(writer))
                    request_message = {"type": "request_peer_list", "server_id": self.server_id}
                    writer.write(json.dumps(request_message).encode() + b'\n')
                    await writer.drain()
                    logging.info("Requested peer list.")
                    asyncio.create_task(self.listen_for_messages(reader, writer))
                    asyncio.create_task(self.send_heartbeat(writer))
                    break
                else:
                    logging.info(f"Unexpected response from {host}:{port}")
                    break
            attempt += 1
            backoff = min(2 ** attempt + random.uniform(0, 1), 60)
            await asyncio.sleep(backoff)
        finally:
            if writer:
                writer.close()
                await writer.wait_closed()

    async def listen_for_messages(self, reader, writer):
        data_buffer = ""  # Initialize a buffer for incoming data
        try:
            while not reader.at_eof():  # Check if the reader has reached EOF
                data = await reader.read(1024)  # Read data from the reader
                if not data:
                    logging.info("No more data received, but connection remains open for new messages.")
                    await asyncio.sleep(1)  # Wait a bit for new data to arrive
                    continue  # Continue listening for new messages instead of closing immediately

                data_buffer += data.decode()
                while '\n' in data_buffer:  # Check if there are complete messages in the buffer
                    line, data_buffer = data_buffer.split('\n', 1)  # Split the buffer into the complete message and the rest
                    if line:  # If there's a complete message
                        try:
                            message = json.loads(line)
                            logging.info(f"Received message: {message}")
                            # Process the message (existing message handling logic goes here)
                        except json.JSONDecodeError as e:
                            logging.warning(f"Error decoding JSON ({e}), data: {line}")
        except Exception as e:
            logging.error(f"Error during message listening: {e}")
        finally:
            logging.info("Reader has reached EOF or an error occurred. Connection remains open for new messages.")
            # Do not immediately close the writer; allow for the possibility of sending more data if protocol permits


    async def handle_client(reader, writer, peer):
        await peer.listen_for_messages(reader, writer)
        writer.close()
        await writer.wait_closed()

    def detect_ip_address(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                return s.getsockname()[0]
        except Exception:
            return '127.0.0.1'

    def rewrite_peers_file(self):
        with open("peers.dat", "w") as f:
            for peer in self.peers:
                f.write(f"{peer}\n")
        logging.info("Peers file updated.")

    def load_peers(self):
        if os.path.exists("peers.dat"):
            with open("peers.dat", "r") as f:
                for line in f:
                    self.peers.add(line.strip())
        logging.info("Peers loaded from file.")

    def add_peer(self, ip):
        if ip in [self.host, self.external_ip, "127.0.0.1"]:
            return
        self.peers.add(ip)
        self.rewrite_peers_file()

    def update_peers(self, new_peers):
        """Adds new peers to the list and updates the peers file."""
        for peer in new_peers:
            self.peers.add(peer)
            logging.info(f"Peer {peer} added to the list.")
        self.rewrite_peers_file()
        
    def respond_to_heartbeat(self, writer, message):
        """Send a heartbeat_ack in response to a heartbeat."""
        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id  # Ensure you have self.server_id defined in your __init__
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        asyncio.create_task(writer.drain())

    async def connect_to_new_peers(self, new_peers):
        for peer_ip in new_peers:
            if peer_ip not in self.peers and peer_ip != self.external_ip:
                self.peers.add(peer_ip)
                self.rewrite_peers_file()
                # Prevent connecting to self.
                if peer_ip == self.detect_ip_address():
                    continue
                logging.info(f"Attempting to connect to new peer: {peer_ip}")
                # Asynchronously attempt to connect to the new peer.
                asyncio.create_task(self.connect_to_peer(peer_ip, self.p2p_port))

    async def send_heartbeat(self, writer):
        """Sends a heartbeat message to the connected peer every 30 seconds."""
        try:
            while not writer.is_closing():
                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id
                }
                logging.info(f"Sending heartbeat to peer.")
                writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                await writer.drain()
                await asyncio.sleep(30)  # Send a heartbeat every 30 seconds.
        except asyncio.CancelledError:
            logging.info("Heartbeat sending task cancelled.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")

    def handle_peer_list(self, peer_data):
        """Processes received peer list and updates internal data structures."""
        try:
            new_peers = [peer for peer in peer_data if peer not in self.peers                                   and peer != self.external_ip] 
            self.update_peers(new_peers)
            asyncio.create_task(self.connect_to_new_peers(new_peers))
        except Exception as e:
            logging.error(f"Error in handle_peer_list: {e}")

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Received message from {addr}: {message}")
        if message.get("type") == "hello":
            logging.info(f"Received handshake from {addr}")
            # Acknowledge the handshake
            ack_message = {
                "type": "ack",
                "payload": "Handshake acknowledged",
                "server_id": self.server_id
            }
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info(f"Handshake acknowledged to {addr}")

        elif message.get("type") == "request_peer_list":
            logging.info(f"Peer list requested by {addr}")
            # Send the peer list
            await self.send_peer_list(writer)

        elif message.get("type") == "heartbeat":
            logging.info(f"Heartbeat received from {addr}")
            # Send a heartbeat acknowledgment
            ack_message = {
                "type": "heartbeat_ack",
                "payload": "pong",
                "server_id": self.server_id
            }
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info("Heartbeat acknowledged to {}".format(addr))

        # Add additional message types as necessary
        else:
            logging.info(f"Unhandled message type from {addr}: {message['type']}")

async def connect_to_server(self):
        while True:
            try:
                reader, writer = await asyncio.open_connection(self.server_host, self.server_port)
                logging.info(f"Connected to server at {self.server_host}:{self.server_port}")
                
                # Example: Send a hello message upon connection
                hello_message = json.dumps({"type": "hello", "payload": "Hello from client"})
                writer.write(hello_message.encode() + b'\n')
                await writer.drain()
                
                # Listen for messages from the server
                await self.listen_for_messages(reader)
                
            except (ConnectionRefusedError, ConnectionResetError):
                logging.error(f"Connection failed. Reconnecting in {self.reconnect_delay} seconds...")
                await asyncio.sleep(self.reconnect_delay)
            except asyncio.CancelledError:
                logging.info("Connection task was cancelled. Exiting.")
                break
            except Exception as e:
                logging.error(f"Unexpected error: {e}")
                break