import asyncio
from peer import Peer
from rpc import RPCServer

async def main():
    host = '127.0.0.1'
    p2p_port = 4333
    rpc_port = 4334
    seeds = ['137.184.80.215']  # Example seed IP

    peer = Peer(host, p2p_port, seeds)
    rpc_server = RPCServer(peer, host, rpc_port)

    # Load existing peers from file
    peer.load_peers()

    await asyncio.gather(
        peer.start_p2p_server(),
        rpc_server.start_rpc_server(),
        *(peer.connect_to_peer(host, p2p_port) for host in seeds)
    )

if __name__ == '__main__':
    asyncio.run(main())
