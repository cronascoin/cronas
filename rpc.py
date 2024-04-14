import asyncio
import json

class AsyncRPCServer:
    def __init__(self, host, port, get_peers_func):
        self.host = host
        self.port = port
        self.get_peers_func = get_peers_func  # Function to get the list of peers
        self.methods = {
            "add": self.add,
            "subtract": self.subtract,
            "list_peers": self.list_peers  # RPC method to list peers
        }

    async def add(self, params):
        return params[0] + params[1]

    async def subtract(self, params):
        return params[0] - params[1]

    async def list_peers(self, params):
        # Call the function passed during initialization to get the list of peers
        return list(self.get_peers_func())

    async def handle_client(self, reader, writer):
        try:
            data = await reader.read(1024)
            request = json.loads(data.decode())
            method = request.get("method")
            params = request.get("params")

            response = {"jsonrpc": "2.0", "id": request.get("id", None)}
            if method in self.methods:
                try:
                    result = await self.methods[method](params)
                    response["result"] = result
                except Exception as e:
                    response["error"] = {"code": -32000, "message": str(e)}
            else:
                response["error"] = {"code": -32601, "message": "Method not found"}

            writer.write(json.dumps(response).encode())
            await writer.drain()
        except Exception as e:
            print(f"Error handling request: {e}")
        finally:
            writer.close()
            await writer.wait_closed()

    async def start(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        addr = server.sockets[0].getsockname()
        print(f'AsyncRPC Server listening on {addr}')

        async with server:
            await server.serve_forever()
