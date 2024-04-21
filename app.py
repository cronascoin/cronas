import asyncio
import logging
from peer import Peer
from rpc import RPCServer

logging.basicConfig(level=logging.INFO)


async def shutdown(peer, rpc_server):
    logging.info("Shutting down...")

    await peer.close_p2p_server()
    await rpc_server.close_rpc_server()

    logging.info("Shutdown complete.")


async def main():
    p2p = '0.0.0.0'
    rpc = '127.0.0.1'
    p2p_port = 4333
    rpc_port = 4334
    seeds = ['137.184.80.215']  # Example seed IP
    
    peer = Peer(p2p, p2p_port, seeds)
    await peer.async_init()  # Ensure asynchronous initialization is called
    rpc_server = RPCServer(peer, rpc, rpc_port)

    try:
        await asyncio.gather(
            peer.start_p2p_server(),
            rpc_server.start_rpc_server(),
            *(peer.connect_to_peer(seed_ip, p2p_port) for seed_ip in seeds)
        )
    
    except asyncio.CancelledError:
        logging.info("CancelledError caught, shutting down.")
        await shutdown(peer, rpc_server)
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt caught, shutting down.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}. Is the server already running?")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("KeyboardInterrupt caught, shutting down.")
    except Exception as e:
        logging.error(f"An unexpected error occurred: {e}. Is the server already running?")
