import asyncio
import datetime
import errno
import json
import logging
import os
import socket
import time
import ipaddress
import aiofiles
import requests
import random
import aiohttp
import xml.etree.ElementTree as ET

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

class Peer:
    def __init__(self, host, p2p_port, server_id, version, max_peers=10, config=None):
        self.server_id = server_id
        self.host = host
        self.p2p_port = p2p_port
        self.external_p2p_port = None  # To store the UPnP-mapped port
        self.peers = {}
        self.active_peers = {}
        self.external_ip = self.detect_ip_address()
        self.out_ip = self.detect_out_ip()
        self.hello_seq = 0
        self.version = version
        self.file_lock = asyncio.Lock()
        self.peers_changed = False
        self.connections = {}
        self.shutdown_flag = False
        self.heartbeat_tasks = {}
        self.file_write_scheduled = False
        self.pending_file_write = False
        self.file_write_delay = 10
        self.max_peers = max_peers
        self.rewrite_lock = asyncio.Lock()
        self.connection_attempts = {}
        self.peers_connecting = set()
        self.config = config
        self.debug = config.get('debug', 'false').lower() == 'true'
        self.p2p_server = None  # Initialize p2p_server
        self.last_peer_list_request = {}  # Track last peer list request times
        self.start_time = datetime.datetime.now()  # Add start time
        self.upnp_device = None  # Initialize UPnP device to None
        self.upnp_service = None  # Initialize UPnP service to None

    async def discover_upnp_devices(self):
        """Discover UPnP devices on the network using sockets."""
        search_target = "urn:schemas-upnp-org:device:InternetGatewayDevice:1"
        message = f"M-SEARCH * HTTP/1.1\r\n" \
                f"HOST: 239.255.255.250:1900\r\n" \
                f"MAN: \"ssdp:discover\"\r\n" \
                f"MX: 3\r\n" \
                f"ST: {search_target}\r\n" \
                f"\r\n"

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
        sock.settimeout(5)

        try:
            # Send the multicast message
            sock.sendto(message.encode('utf-8'), ('239.255.255.250', 1900))

            # Receive responses
            devices = []
            while True:
                try:
                    data, _ = sock.recvfrom(1024)
                    devices.append(data.decode('utf-8'))
                except socket.timeout:
                    break
        finally:
            sock.close()

        return self.parse_upnp_response("\r\n".join(devices))

    def parse_upnp_response(self, response_text):
        """Parse the UPnP discovery response."""
        return [
            line.split(':', 1)[1].strip()
            for line in response_text.split('\r\n')
            if line.lower().startswith('location:')
        ]

    async def get_upnp_device_description(self, location):
        """Retrieve the UPnP device description."""
        async with aiohttp.ClientSession() as session:
            async with session.get(location) as response:
                xml = await response.text()
                return ET.fromstring(xml)

    def find_service(self, device, service_type):
        """Find a specific service within the UPnP device description."""
        return next(
            (
                service
                for service in device.findall(
                    './/{urn:schemas-upnp-org:device-1-0}service'
                )
                if service.find(
                    '{urn:schemas-upnp-org:device-1-0}serviceType'
                ).text
                == service_type
            ),
            None,
        )

    async def add_port_mapping(self, external_port, internal_ip, internal_port, protocol='TCP', description='P2P application'):
        try:
            control_url = self.upnp_service.find('{urn:schemas-upnp-org:device-1-0}controlURL').text
            service_type = self.upnp_service.find('{urn:schemas-upnp-org:device-1-0}serviceType').text

            action = 'AddPortMapping'
            body = f"""
            <?xml version="1.0"?>
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
            <s:Body>
                <u:{action} xmlns:u="{service_type}">
                <NewRemoteHost></NewRemoteHost>
                <NewExternalPort>{external_port}</NewExternalPort>
                <NewProtocol>{protocol}</NewProtocol>
                <NewInternalPort>{internal_port}</NewInternalPort>
                <NewInternalClient>{internal_ip}</NewInternalClient>
                <NewEnabled>1</NewEnabled>
                <NewPortMappingDescription>{description}</NewPortMappingDescription>
                <NewLeaseDuration>0</NewLeaseDuration>
                </u:{action}>
            </s:Body>
            </s:Envelope>
            """

            headers = {
                'Content-Type': 'text/xml',
                'SOAPACTION': f'"{service_type}#{action}"'
            }

            logging.debug(f"Sending SOAP request to {control_url} with headers: {headers} and body: {body}")

            async with aiohttp.ClientSession() as session:
                async with session.post(control_url, data=body, headers=headers) as response:
                    response_text = await response.text()
                    logging.debug(f"Response from UPnP device: {response.status} - {response_text}")

                    if response.status == 200:
                        logging.info(f"Port {external_port} mapped to {internal_ip}:{internal_port}")
                        return external_port
                    else:
                        logging.error(f"Failed to add UPnP port mapping: {response.status} - {response_text}")
                        return None
        except Exception as e:
            logging.error(f"Failed to add UPnP port mapping: {e}")
            return None


    async def remove_port_mapping(self, external_port, protocol='TCP'):
        try:
            control_url = self.upnp_service.find('{urn:schemas-upnp-org:device-1-0}controlURL').text
            service_type = self.upnp_service.find('{urn:schemas-upnp-org:device-1-0}serviceType').text

            action = 'DeletePortMapping'
            body = f"""
            <?xml version="1.0"?>
            <s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">
              <s:Body>
                <u:{action} xmlns:u="{service_type}">
                  <NewRemoteHost></NewRemoteHost>
                  <NewExternalPort>{external_port}</NewExternalPort>
                  <NewProtocol>{protocol}</NewProtocol>
                </u:{action}>
              </s:Body>
            </s:Envelope>
            """

            headers = {
                'Content-Type': 'text/xml',
                'SOAPACTION': f'"{service_type}#{action}"'
            }

            async with aiohttp.ClientSession() as session:
                async with session.post(control_url, data=body, headers=headers) as response:
                    if response.status == 200:
                        logging.info(f"Port {external_port} unmapped")
                    else:
                        logging.error(f"Failed to remove UPnP port mapping: {response.status}")
        except Exception as e:
            logging.error(f"Failed to remove UPnP port mapping: {e}")

    async def initialize_upnp(self):
        logging.info("Initializing UPnP service...")
        devices = await self.discover_upnp_devices()
        if devices:
            self.upnp_device = await self.get_upnp_device_description(devices[0])
            self.upnp_service = self.find_service(self.upnp_device, 'urn:schemas-upnp-org:service:WANIPConnection:1')
            if self.upnp_service:
                self.external_p2p_port = await self.add_port_mapping(self.p2p_port, self.host, self.p2p_port)
                logging.info(f"UPnP port mapping established: {self.external_p2p_port}")
            else:
                logging.info("UPnP service not available.")
        else:
            logging.info("No UPnP devices found.")

    def is_private_ip(self, ip):
        return ipaddress.ip_address(ip).is_private

    async def close_connection(self, host, port):
        if (host, port) in self.connections:
            _, writer = self.connections.pop((host, port))
            if writer and not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Successfully closed connection to {host}:{port}")
        else:
            logging.info(f"No active connection found for {host}:{port} to close.")

    async def close_p2p_server(self):
        if self.p2p_server:
            self.p2p_server.close()
            await self.p2p_server.wait_closed()
            logging.info("P2P server closed.")
            
            if self.upnp_service:
                await self.remove_port_mapping(self.external_p2p_port)

    async def connect_and_maintain(self):
        """Main loop for connecting and maintaining peer connections."""
        while not self.shutdown_flag:
            await self.connect_to_known_peers()
            await asyncio.sleep(60)

    async def connect_to_known_peers(self):
        available_peers = [
            peer for peer in self.peers if peer not in self.active_peers and peer not in self.peers_connecting
        ]
        peers_to_connect = random.sample(available_peers, min(self.max_peers, len(available_peers)))

        self.peers_connecting.update(peers_to_connect)

        tasks = [asyncio.create_task(self.connect_to_peer(host, int(port)))
                 for host, port in (peer.split(':') for peer in peers_to_connect)]
        try:
            await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            self.peers_connecting.difference_update(peers_to_connect)
        self.update_active_peers()

    async def connect_to_new_peers(self, new_peers):
        for peer_info in new_peers:
            try:
                host, port = peer_info.split(':')
                port = int(port)

                if not self.is_valid_peer(peer_info) or peer_info in self.connections or peer_info in self.connection_attempts:
                    continue

                while len(self.connection_attempts) >= self.max_peers:
                    await asyncio.sleep(1)

                self.connection_attempts[peer_info] = 0
                asyncio.create_task(self.connect_to_peer(host, port, 5))
            except ValueError:
                logging.warning(f"Invalid peer address format: {peer_info}")

    async def connect_to_peer(self, host, port, max_retries=5):
        peer_info = f"{host}:{port}"

        if peer_info not in self.connection_attempts:
            self.connection_attempts[peer_info] = 0

        while peer_info in self.connection_attempts and self.connection_attempts[peer_info] < max_retries:
            if self.shutdown_flag:
                logging.info(f"Shutdown in progress, cancelling connection attempts to {peer_info}.")
                return

            if len(self.active_peers) >= self.max_peers:
                logging.info(f"Maximum number of peers ({self.max_peers}) already connected. Skipping connection to {peer_info}.")
                return

            try:
                reader, writer = await asyncio.open_connection(host, port)
                local_addr, local_port = writer.get_extra_info('sockname')

                # Send HELLO message and record the send time
                send_time = time.time()
                await self.send_hello_message(writer)

                if peer_info not in self.connections:
                    await self.request_peer_list(writer, peer_info)

                self.connections[peer_info] = (reader, writer)
                self.active_peers[peer_info] = {
                    'addr': peer_info,
                    'addrlocal': f"{self.out_ip}:{local_port}",
                    'addrbind': f"{self.external_ip}:{local_port}",
                    'server_id': "unknown",  # Initialize with unknown, will be updated upon receiving ack
                    'version': "unknown",    # Initialize with unknown, will be updated upon receiving ack
                    'lastseen': int(time.time()),
                    'ping': None  # Initialize ping with None
                }

                # Store the send time to calculate ping later
                self.active_peers[peer_info]['send_time'] = send_time

                asyncio.create_task(self.listen_for_messages(reader, writer))
                asyncio.create_task(self.send_heartbeat(writer, peer_info))

                self.peers[peer_info] = int(time.time())
                self.peers_changed = True
                await self.schedule_rewrite()
                return

            except Exception as e:
                logging.error(f"Error connecting to {host}:{port}: {e}")
                self.connection_attempts[peer_info] += 1
                await asyncio.sleep(self.connection_attempts[peer_info] * 5)

        if peer_info in self.connection_attempts:
            logging.warning(f"Failed to connect to {host}:{port} after {max_retries} attempts.")
            self.connection_attempts.pop(peer_info, None)
            return

    def detect_ip_address(self):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(('8.8.8.8', 80))
                ip = s.getsockname()[0]
            return ip
        except Exception as e:
            logging.info(f"Failed to detect external IP address: {e}")
            return '127.0.0.1'

    def detect_out_ip(self):
        try:
            response = requests.get('https://api.ipify.org?format=json')
            response.raise_for_status()
            ip = response.json()['ip']
            if ip == '127.0.0.1':
                raise ValueError("Detected out_ip is 127.0.0.1, which is invalid.")
            return ip
        except (requests.RequestException, ValueError) as e:
            logging.error(f"Failed to detect external IP address: {e}")
            return self.get_fallback_ip()

    def get_fallback_ip(self):
        """Fallback method to detect external IP using a different service."""
        try:
            response = requests.get('https://ifconfig.me')
            response.raise_for_status()
            ip = response.text.strip()
            if ip == '127.0.0.1':
                raise ValueError("Fallback detected out_ip is 127.0.0.1, which is invalid.")
            return ip
        except requests.RequestException as e:
            logging.error(f"Fallback method failed to detect external IP address: {e}")
            return '127.0.0.1'

    def get_uptime(self):
        """Calculate and return the uptime in seconds."""
        current_time = datetime.datetime.now()
        uptime = current_time - self.start_time
        return uptime.total_seconds()

    async def handle_ack_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        remote_version = message.get("version", "unknown")
        remote_server_id = message.get("server_id", "unknown")
        local_addr, local_port = writer.get_extra_info('sockname')

        addrlocal = f"{self.out_ip}:{local_port}"
        addrbind = f"{self.external_ip}:{local_port}"

        if peer_info in self.active_peers:
            # Calculate ping time
            receive_time = time.time()
            send_time = self.active_peers[peer_info].get('send_time', receive_time)
            ping = receive_time - send_time

            self.active_peers[peer_info].update({
                "addrlocal": addrlocal,
                "addrbind": addrbind,
                "server_id": remote_server_id,
                "version": remote_version,
                "lastseen": int(time.time()),
                "ping": round(ping, 3)  # Limit ping to 3 decimal places
            })
        else:
            self.active_peers[peer_info] = {
                "addr": peer_info,
                "addrlocal": addrlocal,
                "addrbind": addrbind,
                "server_id": remote_server_id,
                "version": remote_version,
                "lastseen": int(time.time()),
                "ping": None
            }

        logging.info(f"Connection fully established with {peer_info}, version {remote_version}, server_id {remote_server_id}")
        self.update_active_peers()
        self.mark_peer_changed()
        await self.schedule_rewrite()
        asyncio.create_task(self.send_heartbeat(writer))

    async def handle_disconnection(self, peer_info):
        if self.shutdown_flag:
            return

        if peer_info in self.peers:
            self.peers[peer_info] = int(time.time())
            await self.schedule_rewrite()
        if peer_info in self.active_peers:
            del self.active_peers[peer_info]
            logging.info(f"Removed {peer_info} from active peers due to disconnection or error.")
            await self.schedule_reconnect(peer_info)
        self.update_active_peers()

    async def handle_hello_message(self, message, writer):
        addr = writer.get_extra_info('peername')

        # Extract server_id and version from the hello message
        remote_server_id = message.get('server_id', 'unknown')
        remote_version = message.get('version', 'unknown')

        # Construct peer_info using potential listening_port
        if 'listening_port' in message and isinstance(message['listening_port'], int):
            peer_port = message['listening_port']
            peer_info = f"{addr[0]}:{peer_port}"
        else:
            peer_info = f"{addr[0]}:{addr[1]}"

        ack_message = {
            "type": "ack",
            "payload": "Handshake acknowledged",
            "version": self.version,
            "server_id": self.server_id
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        if self.debug:
            logging.info(f"Handshake acknowledged to {peer_info}")

        # Update the active peer's details
        self.active_peers[peer_info] = {
            "addr": peer_info,
            "addrlocal": f"{self.out_ip}:{addr[1]}",  # Use the actual connection port
            "addrbind": f"{self.external_ip}:{addr[1]}",  # Use the actual connection port
            "server_id": remote_server_id,
            "version": remote_version,
            "lastseen": int(time.time()),
            "ping": None
        }

        self.update_active_peers()
        asyncio.create_task(self.send_heartbeat(writer))

    async def handle_peer_connection(self, reader, writer):
        peer_address = writer.get_extra_info('peername')
        peer_info = f"{peer_address[0]}:{peer_address[1]}"

        if peer_info in self.connections:
            logging.info(f"Duplicate connection attempt to {peer_info}. Closing new connection.")
            writer.close()
            await writer.wait_closed()
            return

        self.connections[peer_info] = (reader, writer)

        try:
            buffer = ''
            while True:
                data = await reader.read(1024)
                if not data:
                    break

                buffer += data.decode()
                while '\n' in buffer:
                    message, buffer = buffer.split('\n', 1)
                    if message:
                        message_obj = json.loads(message)
                        await self.process_message(message_obj, writer)

        except asyncio.CancelledError:
            logging.info(f"Connection task with {peer_address} cancelled")
        except ConnectionResetError:
            logging.warning(f"Connection reset by peer {peer_address}.")
        except (asyncio.IncompleteReadError, json.JSONDecodeError):
            logging.warning(f"Connection with {peer_address} closed abruptly or received malformed data.")
        except Exception as e:
            logging.exception(f"General error during P2P communication with {peer_address}: {e}")
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            logging.info(f"Connection with {peer_address} closed.")
            await self.handle_disconnection(peer_info)

    async def handle_peer_list_message(self, message):
        new_peers = message.get("payload", [])
        logging.info("Processing new peer list...")

        valid_new_peers = {
            peer_info: self.peers.get(peer_info, 0) for peer_info in new_peers
            if self.is_valid_peer(peer_info)
        }

        invalid_peers = [
            peer_info for peer_info in new_peers
            if not self.is_valid_peer(peer_info)
        ]

        for invalid_peer in invalid_peers:
            if self.debug:
                logging.warning(f"Invalid peer received: {invalid_peer}")

        self.peers.update(valid_new_peers)
        if valid_new_peers:
            self.peers_changed = True
            await self.schedule_rewrite()

        for peer_info in valid_new_peers:
            try:
                host, port = peer_info.split(':')
                port = int(port)

                if peer_info in self.connections:
                    if self.debug:
                        logging.info(f"Already connected to {peer_info}, skipping additional connection attempt.")
                    continue

                while len(self.connection_attempts) >= self.max_peers:
                    await asyncio.sleep(1)

                self.connection_attempts[peer_info] = 0
                asyncio.create_task(self.connect_to_peer(host, port, 5))
            except ValueError:
                logging.warning(f"Invalid peer address format: {peer_info}")
        self.update_active_peers()

    def is_valid_peer(self, peer_info):
        try:
            ip, port = peer_info.split(':')
            ipaddress.ip_address(ip)

            if ip in [self.host, self.external_ip, self.out_ip, "127.0.0.1", "localhost"]:
                logging.info(f"Skipping self-peer with IP: {ip}")
                return False

            port = int(port)
            return not (32768 <= port <= 65535)
        except ValueError as e:
            logging.error(f"Invalid peer address '{peer_info}': {e}")
            return False

    async def listen_for_messages(self, reader, writer):
        addr = writer.get_extra_info('peername')
        host, port = addr[0], addr[1]
        peer_info = f"{host}:{port}"
        if self.debug:
            logging.info(f"Listening for messages from {peer_info}")
        try:
            while True:
                data_buffer = await reader.readuntil(separator=b'\n')
                if not data_buffer:
                    logging.info("Connection closed by peer.")
                    break
                message = json.loads(data_buffer.decode().strip())
                await self.process_message(message, writer)
        except asyncio.IncompleteReadError:
            logging.info("Incomplete read error, attempting to reconnect...")
            await self.handle_disconnection(peer_info)
            asyncio.create_task(self.reconnect_to_peer(host, int(port)))
        except ConnectionResetError as e:
            logging.error(f"Connection reset error with {peer_info}: {e}")
            await self.handle_disconnection(peer_info)
            asyncio.create_task(self.reconnect_to_peer(host, int(port)))
        except Exception as e:
            logging.error(f"Error during communication with {peer_info}: {e}")
            await self.handle_disconnection(peer_info)
        finally:
            logging.info(f"Closing connection with {peer_info}")
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def load_peers(self):
        loaded_peers_count = 0
        skipped_peers_count = 0
        addnode_set = set(self.config.get('addnode', [])) if self.config else set()

        if os.path.exists("peers.dat"):
            try:
                async with aiofiles.open("peers.dat", "r") as f:
                    async for line in f:
                        try:
                            parts = line.strip().split(":")
                            if len(parts) != 3:
                                raise ValueError("Line format incorrect, expected 3 parts.")

                            peer, port, last_seen_str = parts
                            peer_info = f"{peer}:{port}"
                            last_seen = int(last_seen_str)

                            if self.is_valid_peer(peer_info):
                                self.peers[peer_info] = last_seen
                                loaded_peers_count += 1
                                addnode_set.discard(peer_info)
                            else:
                                skipped_peers_count += 1
                        except ValueError as e:
                            logging.warning(f"Invalid line in peers.dat: {line} - Error: {e}")
                            skipped_peers_count += 1
            except Exception as e:
                logging.error(f"Error reading peers.dat: {e}")
        else:
            self.peers_changed = True

        for peer_info in addnode_set:
            if self.is_valid_peer(peer_info):
                self.peers[peer_info] = int(time.time())
                loaded_peers_count += 1

        await self.schedule_rewrite()
        logging.info(f"Peers loaded from file. Loaded: {loaded_peers_count}, Skipped: {skipped_peers_count}.")

    def mark_peer_changed(self):
        self.peers_changed = True
        logging.debug("Peers data marked as changed.")

    async def process_message(self, message, writer):
        addr = writer.get_extra_info('peername')
        peer_info = f"{addr[0]}:{addr[1]}"
        if self.debug:
            logging.info(f"Received message from {peer_info}: {message}")

        message_type = message.get("type")
        if message_type == "hello":
            await self.handle_hello_message(message, writer)
        elif message_type == "ack":
            await self.handle_ack_message(message, writer)
        elif message_type == "request_peer_list":
            await self.send_peer_list(writer)
        elif message_type == "heartbeat":
            await self.respond_to_heartbeat(writer, message)
        elif message_type == "peer_list":
            await self.handle_peer_list_message(message)

    async def reconnect_to_peer(self, host, port):
        peer_info = f"{host}:{port}"
        await self._attempt_reconnect(peer_info)

    async def schedule_reconnect(self, peer_info):
        await self._attempt_reconnect(peer_info)

    async def _attempt_reconnect(self, peer_info):
        logging.info(f"Attempting to reconnect to {peer_info}")

        retry_intervals = [60, 3600, 86400, 2592000]
        attempt = 0

        while attempt < len(retry_intervals):
            if self.shutdown_flag:
                logging.info(f"Shutdown in progress, cancelling reconnection attempts for {peer_info}.")
                return

            if peer_info not in self.active_peers and peer_info not in self.peers_connecting:
                try:
                    jitter = random.uniform(0.8, 1.2)
                    backoff_time = retry_intervals[attempt] * jitter
                    logging.info(f"Scheduling reconnection attempt for {peer_info} in {backoff_time:.2f} seconds.")
                    await asyncio.sleep(backoff_time)

                    host, port = peer_info.split(':')
                    await self.connect_to_peer(host, int(port))

                    if peer_info in self.active_peers:
                        logging.info(f"Reconnected to {peer_info} successfully.")
                        break
                except Exception as e:
                    logging.error(f"Reconnection attempt {attempt} to {peer_info} failed: {e}")
                    attempt += 1
            else:
                logging.info(f"{peer_info} is already active or attempting to reconnect.")
                break

    async def request_peer_list(self, writer, peer_info):
        current_time = time.time()
        if peer_info in self.last_peer_list_request:
            last_request_time = self.last_peer_list_request[peer_info]
            cooldown_period = 300  # Cooldown period in seconds (e.g., 5 minutes)

            if current_time - last_request_time < cooldown_period:
                logging.info(f"Skipping peer list request to {peer_info} (cooldown period not yet passed)")
                return

        self.last_peer_list_request[peer_info] = current_time
        request_message = {
            "type": "request_peer_list",
            "server_id": self.server_id
        }
        writer.write(json.dumps(request_message).encode() + b'\n')
        await writer.drain()
        logging.info(f"Request for peer list sent to {peer_info}.")

    async def respond_to_heartbeat(self, writer, message):
        peer_info = writer.get_extra_info('peername')
        peer_info_str = f"{peer_info[0]}:{peer_info[1]}"

        if peer_info_str in self.active_peers:
            self.active_peers[peer_info_str]['lastseen'] = int(time.time())
            self.peers_changed = True
            if self.debug:
                logging.info(f"Updated last seen for {peer_info_str}")

        ack_message = {
            "type": "heartbeat_ack",
            "payload": "pong",
            "server_id": self.server_id
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        if self.debug:
            logging.info(f"Sent heartbeat acknowledgment to {peer_info_str}.")

    async def rewrite_peers_file(self):
        if not self.peers_changed:
            return

        async with self.file_lock:
            try:
                valid_peers = dict(self.peers.items())
                async with aiofiles.open("peers.dat", "w") as f:
                    for peer_info, last_seen in valid_peers.items():
                        await f.write(f"{peer_info}:{last_seen}\n")
                self.peers_changed = False
                if self.debug:
                    logging.info("Peers file rewritten successfully.")
            except OSError as e:
                logging.error(f"Failed to open peers.dat: {e}")
            except Exception as e:
                logging.error(f"Failed to rewrite peers.dat: {e}")

    async def send_ack_message(self, writer, version, server_id):
        ack_message = {
            "type": "ack",
            "version": version,
            "server_id": server_id
        }
        writer.write(json.dumps(ack_message).encode() + b'\n')
        await writer.drain()
        logging.info(f"Sent ack message to peer with server_id: {server_id}")

    async def schedule_periodic_peer_save(self):
        while True:
            await asyncio.sleep(60)
            await self.rewrite_peers_file()

    async def schedule_rewrite(self):
        if not self.file_write_scheduled:
            self.file_write_scheduled = True
            asyncio.create_task(self._rewrite_after_delay())

    async def send_heartbeat(self, writer, peer_info=None):
        await asyncio.sleep(60)
        try:
            while not writer.is_closing():
                if self.shutdown_flag:
                    logging.info("Shutdown in progress, stopping heartbeat.")
                    return

                heartbeat_msg = {
                    "type": "heartbeat",
                    "payload": "ping",
                    "server_id": self.server_id,
                    "version": self.version
                }
                writer.write(json.dumps(heartbeat_msg).encode() + b'\n')
                await writer.drain()
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            logging.info("Heartbeat sending cancelled.")
        except Exception as e:
            logging.error(f"Error sending heartbeat: {e}")

    async def send_hello_message(self, writer):
        hello_message = {
            'type': 'hello',
            'host': self.out_ip,  # Use out_ip here
            'port': self.external_p2p_port or self.p2p_port,  # Use external UPnP port if available
            'server_id': self.server_id,
            'version': self.version
        }
        await self.send_message(writer, hello_message)
        if self.debug:
            logging.info(f"Sent hello message: {hello_message}")

    async def send_message(self, writer, message):
        message_data = json.dumps(message).encode('utf-8')
        writer.write(message_data + b'\n')
        await writer.drain()

    async def send_peer_list(self, writer):
        logging.info("Attempting to send peer list...")

        if connecting_ports := [
            peer for peer in self.active_peers.keys() if self.is_valid_peer(peer)
        ]:
            peer_list_message = {
                "type": "peer_list",
                "payload": connecting_ports,
                "server_id": self.server_id,
                "version": self.version
            }

            writer.write(json.dumps(peer_list_message).encode() + b'\n')
            await writer.drain()
            logging.info("Sent active peer list.")
        else:
            logging.warning("No valid active peers to send.")

    async def start(self):
        """Start the peer server and main loop."""
        try:
            if not os.path.exists("peers.dat"):
                self.addnode = self.config.get('addnode', [])
                for peer_info in self.addnode:
                    if self.is_valid_peer(peer_info):
                        self.peers[peer_info] = int(time.time())

            await self.load_peers()

            # Setup UPnP before starting the server
            await self.initialize_upnp()

            await self.start_p2p_server()
            asyncio.create_task(self.connect_and_maintain())
        finally:
            self.shutdown_flag = True
            await self.cleanup_and_rewrite_peers()
            await self.close_p2p_server()

    async def start_p2p_server(self):
        try:
            server = await asyncio.start_server(
                self.handle_peer_connection, self.host, self.p2p_port,
                reuse_address=True
            )
            self.p2p_server = server  # Assign to self.p2p_server
            logging.info(f"P2P server version {self.version} with ID {self.server_id} listening on {self.host}:{self.p2p_port}")
            await self.connect_to_known_peers()
            asyncio.create_task(self.schedule_periodic_peer_save())

            async with server:
                await server.serve_forever()

        except OSError as e:
            logging.error(f"Failed to start server on {self.host}:{self.p2p_port}: {e}")
            if e.errno == errno.EADDRINUSE:
                logging.error(f"Port {self.p2p_port} is already in use. Please ensure the port is free and try again.")
            await self.close_p2p_server()

        except Exception as e:
            logging.error(f"Error starting P2P server: {e}")
            await self.close_p2p_server()

    async def update_last_seen(self, peer_info):
        async with self.rewrite_lock:
            if peer_info in self.peers:
                self.peers[peer_info] = int(time.time())
                self.peers_changed = True
                await self.schedule_rewrite()

    async def _rewrite_after_delay(self):
        await asyncio.sleep(self.file_write_delay)
        if self.peers_changed:
            async with self.file_lock:
                try:
                    valid_peers = {
                        peer_info: last_seen
                        for peer_info, last_seen in self.peers.items()
                        if self.is_valid_peer(peer_info) and last_seen != 0
                    }
                    async with aiofiles.open("peers.dat", "w") as f:
                        for peer_info, last_seen in valid_peers.items():
                            await f.write(f"{peer_info}:{last_seen}\n")
                    self.peers_changed = False
                    if self.debug:
                        logging.info("Peers file rewritten successfully.")
                except OSError as e:
                    logging.error(f"Failed to open peers.dat: {e}")
                except Exception as e:
                    logging.error(f"Failed to rewrite peers.dat: {e}")
        self.file_write_scheduled = False

    async def cleanup_and_rewrite_peers(self):
        """Cleanup peers with last_seen of 0 and rewrite the peers file."""
        self.peers = {peer_info: last_seen for peer_info, last_seen in self.peers.items() if last_seen != 0}
        self.peers_changed = True
        await self.rewrite_peers_file()

        await asyncio.gather(*[self.close_connection(*peer.split(':')) for peer in self.connections.keys()])

    def update_active_peers(self):
        # Sort peers by ping and keep the top 10 fastest
        self.active_peers = dict(sorted(
            self.active_peers.items(),
            key=lambda x: float(x[1]['ping']) if x[1]['ping'] is not None else float('inf')
        )[:self.max_peers])
