# block_reward.py

import asyncio
import json
import logging
import aiofiles

logger = logging.getLogger(__name__)

class BlockReward:
    def __init__(self, crypto, peer, uptime_file, reward_amount):
        """
        Initialize the BlockReward system.

        Args:
            crypto: An instance of the Crypto class handling cryptographic operations.
            peer: An instance of the Peer class for broadcasting transactions.
            uptime_file (str): Path to the uptime tracking file.
            reward_amount (float): The amount to reward per block.
        """
        self.crypto = crypto
        self.peer = peer
        self.reward_amount = reward_amount
        self.uptime_file = uptime_file
        self.reward_lock = asyncio.Lock()  # To prevent concurrent reward issuances
        self.shutdown_event = asyncio.Event()
        self.uptime = 0  # Uptime in seconds

        # Start the uptime tracking task
        self.uptime_task = asyncio.create_task(self.track_uptime())

    async def issue_block_reward(self, receiver_address):
        """
        Create and broadcast a transaction that rewards a node.

        Args:
            receiver_address (str): The blockchain address to receive the reward.

        Returns:
            str: The transaction ID of the issued reward.
        """
        async with self.reward_lock:
            try:
                # Create a transaction to the receiver
                txid = await self.crypto.create_transaction(
                    receiver_address=receiver_address,
                    amount=self.reward_amount
                )
                logger.info(f"Block reward transaction {txid} created for {receiver_address}.")

                # Broadcast the transaction to peers
                await self.broadcast_transaction(txid)
                logger.info(f"Block reward transaction {txid} broadcasted to peers.")

                return txid
            except Exception as e:
                logger.error(f"Failed to issue block reward to {receiver_address}: {e}")
                raise

    async def broadcast_transaction(self, txid):
        """
        Broadcast a transaction to all connected peers.

        Args:
            txid (str): The transaction ID to broadcast.
        """
        try:
            await self.peer.broadcast_transaction(txid)
            logger.debug(f"Broadcasted transaction {txid} to all peers.")
        except AttributeError:
            logger.warning("Peer instance does not have a 'broadcast_transaction' method.")
        except Exception as e:
            logger.error(f"Failed to broadcast transaction {txid}: {e}")

    async def track_uptime(self):
        """
        Track node uptime and periodically save it to the uptime file.
        """
        while not self.shutdown_event.is_set():
            self.uptime += 1  # Increment uptime every second
            await asyncio.sleep(1)

            # Save uptime every minute
            if self.uptime % 60 == 0:
                await self.save_uptime()

    async def save_uptime(self):
        """
        Save the current uptime to the uptime file.
        """
        try:
            uptime_data = {
                "uptime_seconds": self.uptime
            }
            async with aiofiles.open(self.uptime_file, 'w') as f:
                await f.write(json.dumps(uptime_data, indent=4))
            logger.info(f"Uptime {self.uptime} seconds saved to {self.uptime_file}.")
        except Exception as e:
            logger.error(f"Failed to save uptime to {self.uptime_file}: {e}")

    async def shutdown(self):
        """
        Shutdown the BlockReward system gracefully.
        """
        logger.info("BlockReward shutdown initiated.")
        self.shutdown_event.set()

        if not self.uptime_task.done():
            self.uptime_task.cancel()
            try:
                await self.uptime_task
            except asyncio.CancelledError:
                logger.info("Uptime tracking task cancelled.")

        logger.info("BlockReward shutdown complete.")
