import asyncio
import uuid

class AsyncPeerConnection:
    def __init__(self, ip, port, server_id, on_disconnect, on_self_connection_detected):
        self.ip = ip
        self.port = port
        self.server_id = server_id
        self.on_disconnect = on_disconnect
        self.on_self_connection_detected = on_self_connection_detected
        self.writer = None
        self.message_seq = 0
        asyncio.create_task(self.connect())

    async def connect(self):
        try:
            reader, self.writer = await asyncio.open_connection(self.ip, self.port)
            hello_message = f"HELLO {self.server_id}"
            self.writer.write(hello_message.encode())
            await self.writer.drain()
            response = await reader.read(1024)
            if response.decode() == "REJECT":
                print(f"Connection to {self.ip} rejected (self-connection detected).")
                self.writer.close()
                await self.writer.wait_closed()
                self.on_self_connection_detected()
            else:
                print(f"Successfully connected to {self.ip}.")
                asyncio.create_task(self.send_heartbeat())
        except Exception as e:
            print(f"Could not connect to peer at {self.ip}: {e}")
            self.on_disconnect(self.ip)

    async def send_heartbeat(self):
        while True:
            self.message_seq += 1
            heartbeat_message = f"{self.server_id}:{self.message_seq}:HEARTBEAT"
            try:
                self.writer.write(heartbeat_message.encode())
                await self.writer.drain()
                await asyncio.sleep(60)  # Send a heartbeat every 60 seconds.
            except Exception as e:
                print(f"Lost connection to {self.ip}: {e}")
                self.writer.close()
                await self.writer.wait_closed()
                self.on_disconnect(self.ip)
                break

class AsyncIRCServer:
    def __init__(self, host, port):
        self.server_id = str(uuid.uuid4())
        self.host = host
        self.port = port
        self.connected_peers = {}  # Stores AsyncPeerConnection instances
        self.reconnection_delays = {}  # Track reconnection delays for each peer
        self.all_server_ips = [self.determine_outbound_ip()]  # This might need adaptation

    async def determine_outbound_ip(self):
        """Assuming outbound IP determination can be done without actual network call,
        or adapt this method if needed."""
        return 'your_outbound_ip_here'

    async def handle_incoming_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        try:
            hello_message = await reader.read(1024)
            hello_message = hello_message.decode()
            _, incoming_server_id = hello_message.split()
            if incoming_server_id == self.server_id:
                writer.write("REJECT".encode())
                await writer.drain()
                print(f"Rejected self-connection attempt from {addr[0]}.")
            else:
                writer.write("OK".encode())
                await writer.drain()
                print(f"Peer connected from {addr[0]}.")
        except Exception as e:
            print(f"Error handling incoming connection from {addr[0]}: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start_server(self):
        server = await asyncio.start_server(self.handle_incoming_connection, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f'AsyncIRC Server listening on {addr}')

        async with server:
            await server.serve_forever()

    # Additional methods for handling peers, reconnections, etc., need to be adapted for async.
