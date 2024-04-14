import asyncio
from network import AsyncIRCServer  # Adjust the import path as necessary
from rpc import AsyncRPCServer  # Adjust the import path as necessary

async def start_irc_server():
    irc_server = AsyncIRCServer("0.0.0.0", 4333)
    print(f"IRC Server ID: {irc_server.server_id}")
    await irc_server.start_server()

async def start_rpc_server(irc_server):
    # Assuming get_connected_peers is now a property or a synchronous method
    rpc_server = AsyncRPCServer("0.0.0.0", 4334, irc_server.connected_peers)
    await rpc_server.start()

async def main():
    irc_server = AsyncIRCServer("0.0.0.0", 4333)
    print(f"Server ID: {irc_server.server_id}")

    # Start both servers concurrently
    await asyncio.gather(
        start_irc_server(),
        start_rpc_server(irc_server),
    )

if __name__ == "__main__":
    asyncio.run(main())
