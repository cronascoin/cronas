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
        self.peers = set(seeds if seeds else [])  # Initial seed list of peers
        self.hello_seq = 0
        self.external_ip = self.detect_ip_address()
        self.load_peers()
        self.rewrite_peers_file()
            
    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Connected to peer {addr}")
        ip, _ = addr

        if ip == self.host or ip == self.external_ip or ip == "127.0.0.1":
            logging.info("Detected attempt to connect to self. Ignoring.")
            writer.close()
            await writer.wait_closed()
            return

        try:
            while True:
                data = await reader.readline()
                if not data:
                    logging.info(f"Connection closed by peer {addr}")
                    break

                message = json.loads(data.decode())
                logging.info(f"Received message from {addr}: {message}")

                if message.get("type") == "hello":
                    logging.info(f"Received handshake from {addr}")
                    self.add_peer(ip)
                    ack_message = {"type": "ack", "payload": "Handshake acknowledged"}
                    writer.write(json.dumps(ack_message).encode() + b'\n')
                    await writer.drain()
                    logging.info(f"Handshake acknowledged to {addr}")
                elif message.get("type") == "request_peer_list":
                    logging.info(f"Peer list requested by {addr}")
                    await self.send_peer_list(writer)
                else:
                    logging.info(f"Unhandled message type from {addr}: {message['type']}")

        except asyncio.TimeoutError:
            logging.info(f"Timeout while waiting for messages from {addr}")
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
        try:
            while True:
                data = await reader.readline()
                if not data:
                    logging.info("Peer disconnected.")
                    break
                message = json.loads(data.decode())
                logging.info(f"Received message: {message}")
                if message.get("type") == "peer_list":
                    logging.info("Received peer list.")
                    new_peers = message.get("payload", [])
                    for peer in new_peers:
                        if peer != self.detect_ip_address():  # Avoid adding self to the peer list
                            self.add_peer(peer)
                    self.rewrite_peers_file()
                elif message.get("type") == "heartbeat_ack":
                    logging.info("Heartbeat acknowledgment received.")
                # Handle other message types as necessary
        except Exception as e:
            logging.error(f"Error in communication: {e}")

    async def send_heartbeat(self, writer):
        while not writer.is_closing():
            heartbeat_msg = {
                "type": "heartbeat",
                "payload": "ping",
                "server_id": self.server_id
            }
            logging.info("Sending heartbeat.")
            writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
            await writer.drain()
            await asyncio.sleep(30)  # Adjust the frequency as needed

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
