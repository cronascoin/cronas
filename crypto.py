# crypto.py

import json
import logging
import os
import time
import hashlib
from typing import Dict
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding

logger = logging.getLogger(__name__)

class Crypto:
    def __init__(self, blockchain_file, utxo_file, wallet_file):
        self.blockchain_file = blockchain_file
        self.utxo_file = utxo_file
        self.wallet_file = wallet_file

        self.private_key = None
        self.public_key_pem = None
        self.blockchain = []
        self.utxo_pool = []
        self.address = ""
        self.public_key = ""
        self.peer_public_keys = {}

    def load_or_generate_private_key(self):
        """
        Loads the private key from the wallet file or generates a new one.
        If loading fails due to corruption or incorrect format, rename the old wallet file
        by incrementing a suffix number until an unused name is found, then generate a new key.
        """
        if os.path.exists(self.wallet_file):
            try:
                with open(self.wallet_file, "rb") as key_file:
                    private_key = serialization.load_pem_private_key(
                        key_file.read(),
                        password=None,
                    )
                logger.info("Loaded existing private key from wallet.")
                return private_key
            except Exception as e:
                logger.error(f"Failed to load private key: {e}")
                # The wallet file is corrupted or invalid. Rename it and generate a new key.
                self.rename_corrupted_wallet(self.wallet_file)

        # Generate a new private key if none found or corrupted
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
        )
        # Save the private key
        try:
            with open(self.wallet_file, "wb") as key_file:
                key_file.write(
                    private_key.private_bytes(
                        encoding=serialization.Encoding.PEM,
                        format=serialization.PrivateFormat.TraditionalOpenSSL,
                        encryption_algorithm=serialization.NoEncryption()
                    )
                )
            logger.info("Generated and saved new private key to wallet.")
        except Exception as e:
            logger.error(f"Failed to save new private key: {e}")
        return private_key

    def rename_corrupted_wallet(self, filename):
        """
        Renames the existing corrupted wallet file by incrementing a suffix number until an unused name is found.
        For example, if wallet.dat is corrupted:
        - Try wallet1.dat, if exists then try wallet2.dat, and so on.
        """
        self._extracted_from__handle_corrupted_file_7(
            filename,
            'Renamed corrupted wallet from ',
            'Failed to rename corrupted wallet file: ',
        )

    def load_blockchain(self):
        """
        Load the blockchain from the specified file.
        Initializes a new blockchain if the file doesn't exist or is corrupted.
        """
        if os.path.exists(self.blockchain_file):
            try:
                with open(self.blockchain_file, 'r') as f:
                    self.blockchain = json.load(f)
                logger.info(f"Loaded blockchain from {self.blockchain_file}")
            except (json.JSONDecodeError, OSError) as e:
                logger.error(f"Blockchain file corrupted: {e}")
                self._handle_corrupted_file(self.blockchain_file)
                self._extracted_from_load_blockchain_14()
        else:
            self._extracted_from_load_blockchain_14()

    # TODO Rename this here and in `load_blockchain`
    def _extracted_from_load_blockchain_14(self):
        self.blockchain = []
        self._save_to_file(self.blockchain_file, self.blockchain)
        logger.info(f"Initialized new blockchain at {self.blockchain_file}")

    def load_utxos(self):
        """
        Load the UTXO pool from the specified file.
        Initializes an empty pool if the file doesn't exist or is corrupted.
        """
        if os.path.exists(self.utxo_file):
            try:
                with open(self.utxo_file, 'r') as f:
                    self.utxo_pool = json.load(f)
                logger.info(f"Loaded UTXO pool from {self.utxo_file}")
            except (json.JSONDecodeError, OSError) as e:
                logger.error(f"UTXO file corrupted: {e}")
                self._handle_corrupted_file(self.utxo_file)
                self._extracted_from_load_utxos_14()
        else:
            self._extracted_from_load_utxos_14()

    # TODO Rename this here and in `load_utxos`
    def _extracted_from_load_utxos_14(self):
        self.utxo_pool = []
        self._save_to_file(self.utxo_file, self.utxo_pool)
        logger.info(f"Initialized new UTXO pool at {self.utxo_file}")

    def load_wallet(self):
        """
        Load the wallet from the specified file.
        Creates a new wallet if the file doesn't exist or is corrupted.
        """
        if os.path.exists(self.wallet_file):
            try:
                self._load_wallet_data()
            except (KeyError, json.JSONDecodeError) as e:
                self._extracted_from_load_wallet_10('Wallet file corrupted: ', e)
            except Exception as e:
                self._extracted_from_load_wallet_10('Error loading wallet: ', e)
        else:
            self._create_new_wallet()

    # TODO Rename this here and in `load_wallet`
    def _extracted_from_load_wallet_10(self, arg0, e):
        logger.error(f"{arg0}{e}")
        self._handle_corrupted_file(self.wallet_file)
        self._create_new_wallet()

    def _load_wallet_data(self):
        with open(self.wallet_file, 'r') as f:
            wallet_data = json.load(f)
        self.private_key = serialization.load_pem_private_key(
            wallet_data['private_key'].encode('utf-8'),
            password=None,
            backend=default_backend()
        )
        self.address = wallet_data['wallet_address']
        self.public_key = wallet_data.get('public_key')
        if not self.public_key:
            self.public_key = self.generate_public_key()
            wallet_data['public_key'] = self.public_key
            self.save_wallet(wallet_data)
            logger.info("Added 'public_key' to existing wallet.")
        logger.info(f"Loaded wallet from {self.wallet_file}")

    def _create_new_wallet(self):
        """
        Create a new wallet with a private key, public key, and address.
        """
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        self.public_key = self.generate_public_key()
        self.address = self.generate_address()
        wallet_data = {
            'wallet_address': self.address,
            'private_key': self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ).decode('utf-8'),
            'public_key': self.public_key
        }
        self.save_wallet(wallet_data)
        logger.info(f"Created new wallet at {self.wallet_file}")

    def generate_public_key(self) -> str:
        """
        Generates the public key from the private key.
        """
        return self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

    def generate_address(self) -> str:
        """
        Generates the wallet address using SHA256 of the public key.
        """
        public_key_bytes = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        digest = hashes.Hash(hashes.SHA256(), backend=default_backend())
        digest.update(public_key_bytes)
        return digest.finalize().hex()

    def save_wallet(self, wallet_data: dict):
        """
        Save the wallet data to the specified file.
        """
        self._save_to_file(self.wallet_file, wallet_data)

    @staticmethod
    def _save_to_file(filename: str, data):
        """
        Save data to a file in JSON format.
        """
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save data to {filename}: {e}")
            raise

    def _handle_corrupted_file(self, filename):
        """
        Handle a corrupted file by renaming it to a new name with incremented suffixes.
        """
        self._extracted_from__handle_corrupted_file_7(
            filename,
            'Renamed corrupted file from ',
            'Failed to rename corrupted file: ',
        )

    # TODO Rename this here and in `rename_corrupted_wallet` and `_handle_corrupted_file`
    def _extracted_from__handle_corrupted_file_7(self, filename, arg1, arg2):
        base_name, ext = os.path.splitext(filename)
        suffix = 1
        while True:
            new_name = f"{base_name}{suffix}{ext}"
            if not os.path.exists(new_name):
                try:
                    os.rename(filename, new_name)
                    logger.info(f"{arg1}{filename} to {new_name}")
                except Exception as e:
                    logger.error(f"{arg2}{e}")
                break
            suffix += 1

    def add_peer_public_key(self, server_id, public_key_pem):
        """
        Adds a peer's public key to the storage.

        Args:
            server_id (str): The server_id of the peer.
            public_key_pem (str): The PEM-encoded public key of the peer.
        """
        try:
            public_key = serialization.load_pem_public_key(
                public_key_pem.encode('utf-8'),
                backend=default_backend()
            )
            self.peer_public_keys[server_id] = public_key
            logger.info(f"Added public key for peer {server_id}.")
        except Exception as e:
            logger.error(f"Failed to load public key for peer {server_id}: {e}")

    def get_public_key_pem(self):
        """
        Retrieves the public key in PEM format.
        """
        public_key = self.private_key.public_key()
        return public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')

    def generate_txid(self) -> str:
        """
        Generate a unique transaction ID.
        """
        return f"tx{int(time.time() * 1000)}"

    def sign(self, data):
        """
        Signs the provided data using the private key.
        """
        return self.private_key.sign(
            data,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )

    def verify(self, data, signature, server_id):
        """
        Verifies the signature of the data using the peer's public key.
        """
        public_key = self.peer_public_keys.get(server_id)
        if not public_key:
            logger.warning(f"No public key found for peer {server_id}. Cannot verify signature.")
            return False
        try:
            public_key.verify(
                signature,
                data,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception as e:
            logger.warning(f"Signature verification failed for peer {server_id}: {e}")
            return False

    def verify_block(self, block_data: dict) -> bool:
        """
        Verify the integrity and validity of a block.
        """
        try:
            expected_hash = self._compute_block_hash(block_data)
            return block_data.get("block_hash") == expected_hash
        except Exception as e:
            logger.error(f"Block verification failed: {e}")
            return False

    def _compute_block_hash(self, block_data: dict) -> str:
        """
        Compute the SHA256 hash of the block data.
        """
        block_string = json.dumps(block_data, sort_keys=True).encode('utf-8')
        return hashlib.sha256(block_string).hexdigest()

    async def reward_node(self, generator_address: str):
        """
        Reward the node that generated a valid block.
        """
        reward_amount = 50.0
        try:
            txid = await self.create_transaction(generator_address, reward_amount)
            logger.info(f"Rewarded node {generator_address} with {reward_amount} units in transaction {txid}.")
        except ValueError as e:
            logger.error(f"Failed to reward node {generator_address}: {e}")

    async def create_transaction(self, receiver_address: str, amount: float) -> str:
        """
        Create a transaction with the specified receiver address and amount.
        """
        # If you need a lock for concurrency, define self.lock somewhere (like in __init__) and use async with self.lock.
        total = 0.0
        selected_utxos = []
        for utxo in self.utxo_pool:
            if utxo['address'] == self.address:
                selected_utxos.append(utxo)
                total += utxo['amount']
                if total >= amount:
                    break
        if total < amount:
            raise ValueError("Insufficient funds.")
        txid = self.generate_txid()
        transaction = {
            "txid": txid,
            "inputs": [{"txid": u['txid'], "vout": u['vout']} for u in selected_utxos],
            "outputs": [
                {"address": receiver_address, "amount": amount},
                {"address": self.address, "amount": round(total - amount, 8)}
            ]
        }
        for u in selected_utxos:
            self.utxo_pool.remove(u)
        for index, output in enumerate(transaction['outputs']):
            self.utxo_pool.append({
                "txid": txid,
                "vout": index,
                "address": output['address'],
                "amount": output['amount']
            })
        self._save_to_file(self.utxo_file, self.utxo_pool)
        self.blockchain.append(transaction)
        self._save_to_file(self.blockchain_file, self.blockchain)
        logger.info(f"Created transaction {txid} sending {amount} to {receiver_address}")
        return txid

    async def broadcast_block(self, block: dict, peer_connections: Dict[str, any]):
        """
        Broadcast a newly created block to all connected peers.
        (Implementation depends on how you send messages in peer.py or message.py)
        """
        # This method can be implemented once you have a method in peer or message to send blocks.
        # For now, this can remain as a placeholder or integrated later.
        pass

    async def initialize(self):
        """
        Initialize the Crypto module by loading blockchain, UTXO, and wallet data.
        """
        logger.info("Initializing Crypto module...")
        self.private_key = self.load_or_generate_private_key()
        self.public_key_pem = self.get_public_key_pem()
        self.load_blockchain()
        self.load_utxos()
        self.load_wallet()
        logger.info("Crypto module initialized successfully.")

    async def shutdown(self):
        """
        Perform any necessary cleanup during shutdown.
        """
        logger.info("Crypto module shutdown initiated.")
        # Add any cleanup tasks here if necessary
        logger.info("Crypto module shutdown complete.")
