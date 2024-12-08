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
        self.certificate = None
        self.wallet_address = ""
        self.public_key_pem = ""
        self.peer_public_keys = {}

    def load_or_generate_wallet(self):
        """
        Loads the wallet from the wallet file or generates a new one.
        If loading fails due to corruption or incorrect format, rename the old wallet file
        by incrementing a suffix number until an unused name is found, then generate a new wallet.
        """
        if os.path.exists(self.wallet_file):
            try:
                return self._extracted_from_load_or_generate_wallet_9()
            except (json.JSONDecodeError, KeyError, ValueError, TypeError) as e:
                logger.error(f"Failed to load wallet: {e}")
                self.rename_corrupted_file(self.wallet_file)
            except Exception as e:
                logger.error(f"Unexpected error loading wallet: {e}")
                self.rename_corrupted_file(self.wallet_file)

        # Generate a new wallet if none found or corrupted
        self.generate_new_wallet()

    # TODO Rename this here and in `load_or_generate_wallet`
    def _extracted_from_load_or_generate_wallet_9(self):
        with open(self.wallet_file, "r") as f:
            wallet_data = json.load(f)

        # Load Private Key
        self.private_key = serialization.load_pem_private_key(
            wallet_data['private_key'].encode('utf-8'),
            password=None,
            backend=default_backend()
        )

        # Load Certificate
        self.certificate = wallet_data['certificate']

        # Load Wallet Address
        self.wallet_address = wallet_data['wallet_address']

        logger.info("Loaded existing wallet from wallet.dat.")
        return

    def rename_corrupted_file(self, filename):
        """
        Renames the existing corrupted file by incrementing a suffix number until an unused name is found.
        For example, if wallet.dat is corrupted:
        - Try wallet1.dat, if exists then try wallet2.dat, and so on.
        """
        base_name, ext = os.path.splitext(filename)
        suffix = 1
        while True:
            new_name = f"{base_name}{suffix}{ext}"
            if not os.path.exists(new_name):
                try:
                    os.rename(filename, new_name)
                    logger.info(f"Renamed corrupted file from {filename} to {new_name}")
                except Exception as e:
                    logger.error(f"Failed to rename corrupted file: {e}")
                break
            suffix += 1

    def generate_new_wallet(self):
        """
        Generates a new private key, certificate, and wallet address.
        Saves them to the wallet file.
        """
        # Generate Private Key
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        
        # Generate Public Key PEM
        self.public_key_pem = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')
        
        # Generate Wallet Address
        self.wallet_address = self.generate_wallet_address()
        
        # Generate Certificate (Placeholder)
        # In a real-world scenario, you'd obtain a certificate from a Certificate Authority or generate a self-signed one.
        self.certificate = self.generate_certificate()

        # Save to wallet.dat
        wallet_data = {
            'private_key': self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,
                encryption_algorithm=serialization.NoEncryption()
            ).decode('utf-8'),
            'certificate': self.certificate,
            'wallet_address': self.wallet_address
        }

        self.save_wallet(wallet_data)
        logger.info("Generated and saved new wallet to wallet.dat.")

    def generate_wallet_address(self) -> str:
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

    def generate_certificate(self) -> str:
        """
        Generates a self-signed certificate.
        NOTE: For demonstration purposes only. In production, use certificates from a trusted CA.
        """
        from cryptography import x509
        from cryptography.x509.oid import NameOID
        import datetime

        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"California"),
            x509.NameAttribute(NameOID.LOCALITY_NAME, u"San Francisco"),
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"Cronas Network"),
            x509.NameAttribute(NameOID.COMMON_NAME, u"cronas.network"),
        ])

        certificate = x509.CertificateBuilder().subject_name(
            subject
        ).issuer_name(
            issuer
        ).public_key(
            self.private_key.public_key()
        ).serial_number(
            x509.random_serial_number()
        ).not_valid_before(
            datetime.datetime.utcnow()
        ).not_valid_after(
            # Certificate valid for 10 years
            datetime.datetime.utcnow() + datetime.timedelta(days=3650)
        ).add_extension(
            x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
            critical=False,
        ).sign(self.private_key, hashes.SHA256(), default_backend())

        return certificate.public_bytes(serialization.Encoding.PEM).decode('utf-8')

    def save_wallet(self, wallet_data: dict):
        """
        Saves the wallet data to the wallet file in JSON format.
        """
        try:
            with open(self.wallet_file, 'w') as f:
                json.dump(wallet_data, f, indent=4)
            logger.info(f"Wallet data saved to {self.wallet_file}")
        except Exception as e:
            logger.error(f"Failed to save wallet data: {e}")
            raise e

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
                self.rename_corrupted_file(self.blockchain_file)
                self._extracted_from_load_blockchain_14()
        else:
            self._extracted_from_load_blockchain_14()

    # TODO Rename this here and in `load_blockchain`
    def _extracted_from_load_blockchain_14(self):
        self.blockchain = []
        self.save_to_file(self.blockchain_file, self.blockchain)
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
                self.rename_corrupted_file(self.utxo_file)
                self._extracted_from_load_utxos_14()
        else:
            self._extracted_from_load_utxos_14()

    # TODO Rename this here and in `load_utxos`
    def _extracted_from_load_utxos_14(self):
        self.utxo_pool = []
        self.save_to_file(self.utxo_file, self.utxo_pool)
        logger.info(f"Initialized new UTXO pool at {self.utxo_file}")

    def save_to_file(self, filename: str, data):
        """
        Save data to a file in JSON format.
        """
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save data to {filename}: {e}")
            raise e

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
            expected_hash = self.compute_block_hash(block_data)
            return block_data.get("block_hash") == expected_hash
        except Exception as e:
            logger.error(f"Block verification failed: {e}")
            return False

    def compute_block_hash(self, block_data: dict) -> str:
        """
        Compute the SHA256 hash of the block data.
        """
        block_string = json.dumps(block_data, sort_keys=True).encode('utf-8')
        return hashlib.sha256(block_string).hexdigest()

    async def create_transaction(self, receiver_address: str, amount: float) -> str:
        """
        Create a transaction with the specified receiver address and amount.
        """
        # Implement concurrency control if needed (e.g., asyncio.Lock)
        total = 0.0
        selected_utxos = []
        for utxo in self.utxo_pool:
            if utxo['address'] == self.wallet_address:
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
                {"address": self.wallet_address, "amount": round(total - amount, 8)}
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
        self.save_to_file(self.utxo_file, self.utxo_pool)
        self.blockchain.append(transaction)
        self.save_to_file(self.blockchain_file, self.blockchain)
        logger.info(f"Created transaction {txid} sending {amount} to {receiver_address}")
        return txid

    async def broadcast_block(self, block: dict, peer_connections: Dict[str, any]):
        """
        Broadcast a newly created block to all connected peers.
        (Implementation depends on how you send messages in peer.py or message.py)
        """
        # Placeholder for block broadcasting logic
        pass

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

    async def initialize(self):
        """
        Initialize the Crypto module by loading blockchain, UTXO, and wallet data.
        """
        logger.info("Initializing Crypto module...")
        self.load_or_generate_wallet()
        self.load_blockchain()
        self.load_utxos()
        logger.info("Crypto module initialized successfully.")

    async def shutdown(self):
        """
        Perform any necessary cleanup during shutdown.
        """
        logger.info("Crypto module shutdown initiated.")
        # Add any cleanup tasks here if necessary
        logger.info("Crypto module shutdown complete.")
