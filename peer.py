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
        # Detect server's external IP address once and store it
        self.external_ip = self.detect_ip_address()
        self.load_peers()
        self.rewrite_peers_file()

    async def handle_peer_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        logging.info(f"Connected to peer {addr}")
        ip, _ = addr

        # Prevent the server from adding or communicating with itself
        if ip == self.host or ip == self.external_ip or ip == "127.0.0.1":
            logging.info("Detected attempt to connect to self. Ignoring.")
            writer.close()
            await writer.wait_closed()
            return

        data = await reader.readline()
        message = json.loads(data.decode())
        
        if message.get("type") == "hello":
            logging.info(f"Received handshake from {addr}")
            self.add_peer(ip)  # Add the new peer
            ack_message = {"type": "ack", "payload": "Handshake acknowledged"}
            writer.write(json.dumps(ack_message).encode() + b'\n')
            await writer.drain()
            logging.info(f"Handshake acknowledged to {addr}")
        
        while True:
            logging.info(f"Starting heartbeat loop to {host}:{port}")
            try:
                while True:
                    heartbeat_msg = {"type": "heartbeat", "payload": "ping"}
                    logging.info(f"Sending heartbeat to {host}:{port}")
                    writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                    await writer.drain()
                    
                    # Log when waiting for heartbeat acknowledgment
                    logging.info(f"Waiting for heartbeat ack from {host}:{port}")
                    ack_data = await asyncio.wait_for(reader.readline(), timeout=300)  # Adjust based on expected frequency
                    if ack_data:
                        logging.info(f"Received heartbeat ack from {host}:{port}")
                    else:
                        logging.error(f"No heartbeat ack from {host}:{port}, closing connection.")
                        break
                    
                    await asyncio.sleep(60)  # Adjust frequency as needed
            except asyncio.TimeoutError:
                logging.error(f"Heartbeat ack not received within the expected timeframe from {host}:{port}.")
            finally:
                writer.close()
                await writer.wait_closed()

    async def start_p2p_server(self):
        server = await asyncio.start_server(self.handle_peer_connection, self.host, self.p2p_port)
        logging.info(f"P2P server {self.server_id} listening on {self.host}:{self.p2p_port}")
        async with server:
            await server.serve_forever()

    async def connect_to_peer(self, host, port, max_retries=5):
        # Prevent self-connection
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
        # Check against all possible self-IPs before adding
        if ip in [self.host, self.external_ip, "127.0.0.1"]:
            return
        self.peers.add(ip)
        self.rewrite_peers_file()
