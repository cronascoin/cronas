import asyncio
import json
import logging
import os
import socket
import uuid

logging.basicConfig(level=logging.INFO)

class Peer:
    def __init__(self, host, p2p_port, seeds=None):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.p2p_port = p2p_port
        self.peers = set(seeds if seeds else [])
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

        # Continuously listen for and process messages
        await self.listen_for_messages(reader, writer, addr)

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

                # After handshake, start listening for messages and sending heartbeats
                asyncio.create_task(self.listen_for_messages(reader, writer, (host, port)))
                asyncio.create_task(self.send_heartbeat(writer))
                break
        finally:
            if writer:
                logging.info("Connection setup completed with {}:{}".format(host, port))

    async def listen_for_messages(self, reader, writer, addr):
        try:
            while True:
                data = await reader.readline()
                if not data:
                    logging.info(f"Connection closed by peer {addr}")
                    break
                message = json.loads(data.decode())
                logging.info(f"Received message from {addr}: {message}")
                # Handle heartbeat messages
                if message.get("type") == "heartbeat":
                    logging.info(f"Heartbeat received from {addr}")
                    response = {"type": "heartbeat_ack", "payload": "pong"}
                    writer.write(json.dumps(response).encode() + b'\n')
                    await writer.drain()
                else:
                    logging.info(f"Unhandled message type from {addr}: {message['type']}")
        except Exception as e:
            logging.error(f"Error during communication with {addr}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def send_heartbeat(self, writer):
        while not writer.is_closing():
            heartbeat_msg = {"type": "heartbeat", "payload": "ping", "server_id": self.server_id}
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
