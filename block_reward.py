# block_reward.py

import asyncio
import datetime
import time
import logging

logger = logging.getLogger(__name__)

class BlockReward:
    def __init__(self, crypto):
        self.crypto = crypto
        self.total_uptime = 0.0  # Total uptime for the current session (in-memory)
        self.daily_uptime = 0.0  # Daily uptime, resets at 00:00 GMT
        self.last_update_time = time.time()
        self.last_reset_time = self.get_next_reset_time()
        self.uptime_task = None  # Task for uptime tracking
        self.shutdown_flag = False

    def get_next_reset_time(self):
        """Get the next reset time at 00:00 GMT."""
        now = datetime.datetime.now(datetime.timezone.utc)
        next_midnight = (now + datetime.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        return next_midnight.timestamp()

    async def uptime_tracking(self):
        """Continuously track and update the node's uptime."""
        try:
            while not self.shutdown_flag:
                current_time = time.time()
                elapsed_time = current_time - self.last_update_time

                # Update total uptime (in-memory, resets on restart)
                self.total_uptime += elapsed_time

                # Update daily uptime
                self.daily_uptime += elapsed_time

                # Check if it's time to reset daily uptime (at 00:00 GMT)
                datetime.datetime.now(datetime.timezone.utc)
                if current_time >= self.last_reset_time:
                    # Before resetting, check if daily uptime was 24 hours
                    if self.daily_uptime >= 24 * 3600:
                        await self.create_coinbase_transaction()
                    else:
                        logger.info("Daily uptime less than 24 hours. Not eligible for block reward.")

                    # Reset daily uptime
                    self.daily_uptime = 0.0
                    # Update last reset time to the next midnight
                    self.last_reset_time = self.get_next_reset_time()
                    logger.info("Daily uptime reset at 00:00 GMT.")

                self.last_update_time = current_time
                await asyncio.sleep(60)  # Update every 60 seconds
        except asyncio.CancelledError:
            logger.info("Uptime tracking task cancelled.")
            # Perform any necessary cleanup here
            raise
        except Exception as e:
            logger.error(f"Error in uptime_tracking: {e}", exc_info=True)

    async def create_coinbase_transaction(self):
        """Create a coinbase transaction awarding the node for daily uptime."""
        try:
            if self.crypto:
                await self.crypto.create_coinbase_transaction()
                logger.info("24 hours of daily uptime reached. Coinbase transaction created.")
            else:
                logger.warning("Crypto module is not available to create coinbase transaction.")
        except asyncio.CancelledError:
            logger.info("create_coinbase_transaction was cancelled.")
            raise
        except Exception as e:
            logger.error(f"Failed to create coinbase transaction: {e}", exc_info=True)

    async def start(self):
        """Start the uptime tracking task."""
        self.uptime_task = asyncio.create_task(self.uptime_tracking())
        logger.info("Uptime tracking task started.")

    async def shutdown(self):
        """Shut down the uptime tracking task."""
        logger.info("Shutting down BlockReward...")
        self.shutdown_flag = True
        if self.uptime_task:
            self.uptime_task.cancel()
            try:
                await self.uptime_task
                logger.info("Uptime tracking task cancelled.")
            except asyncio.CancelledError:
                logger.info("Uptime tracking task cancelled via exception.")
            except Exception as e:
                logger.error(f"Error cancelling uptime tracking task: {e}")
        logger.info("BlockReward shutdown complete.")
