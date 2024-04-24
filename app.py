#app.py

import asyncio
import contextlib
import logging
from peer import Peer
from rpc import RPCServer
 
logging.basicConfig(level=logging.INFO)

async def shutdown(peer, rpc_server):
    logging.info("Shutting down...")
    
    await peer.cancel_heartbeat_tasks()  # Ensure heartbeat tasks are cancelled first
    await peer.close_p2p_server()
    await rpc_server.close_rpc_server()
    
    await cancel_remaining_tasks()  # Cancel any remaining asyncio tasks
    
    logging.info("Shutdown complete.")

async def cancel_remaining_tasks():
    for task in asyncio.all_tasks():
        if task is not asyncio.current_task():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task  # Await task to allow it to cancel gracefully

async def main():
    p2p_host = '0.0.0.0'
    rpc_host = '127.0.0.1'
    p2p_port = 4333
    rpc_port = 4334
    seeds = ['137.184.80.215:4333']  # Assuming seeds include port information
    
    # Create and initialize the Peer object
    peer = Peer(p2p_host, p2p_port, seeds)
    await peer.async_init()  # Ensures the Peer object is fully initialized before proceeding
    
    # Initialize the RPC server with the already initialized Peer object
    rpc_server = RPCServer(peer, rpc_host, rpc_port)

    try:
        # Start the P2P and RPC servers along with connecting to seed peers, concurrently
        await asyncio.gather(
            peer.start_p2p_server(),
            rpc_server.start_rpc_server(),
            # Updated to ensure that the connect_to_peer function is awaited for each seed
            *(peer.connect_to_peer(seed.split(':')[0], int(seed.split(':')[1])) for seed in seeds)
        )
    
    except asyncio.CancelledError:
        # Catching CancelledError to handle asyncio task cancellations
        logging.info("CancelledError caught, shutting down.")
        await shutdown(peer, rpc_server)
    except KeyboardInterrupt:
        logging.info("Received KeyboardInterrupt. Initiating shutdown...")
        await shutdown(peer, rpc_server) # Assuming you have a shutdown function
    except Exception as e:
        # General exception catch to handle unexpected errors
        logging.error(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    asyncio.run(main())
