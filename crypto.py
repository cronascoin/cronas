# crypto.py

import hashlib
import json
import logging
from datetime import datetime, timezone
import aiosqlite
import os
import getpass  # For secure passphrase input
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import (
    serialization,
    hashes,
    padding  # Import padding for PKCS7
)
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature, InvalidKey

logger = logging.getLogger(__name__)

class Crypto:
    def __init__(self, peer):
        self.peer = peer
        self.utxo_pool = {}  # Key: UTXO ID, Value: UTXO data
        self.transaction_pool = []  # List of pending transactions

        self.shutdown_flag = False

        # Initialize the database connection (will be set in init_blockchain_db)
        self.conn = None
        self.cursor = None

        # Wallet attributes
        self.wallet_address = None
        self.private_key = None

    async def init_blockchain_db(self):
        """Initialize the SQLite database for storing blockchain data."""
        self.conn = await aiosqlite.connect("blockchain.db")
        self.conn.row_factory = aiosqlite.Row  # Enable access by column name
        self.cursor = await self.conn.cursor()

        # Create tables if they don't exist
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS blocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                block_hash TEXT UNIQUE,
                previous_hash TEXT,
                timestamp TEXT
            )
            """
        )
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_id TEXT UNIQUE,
                block_hash TEXT,
                sender TEXT,
                receiver TEXT,
                amount REAL,
                timestamp TEXT,
                signature TEXT
            )
            """
        )
        await self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS utxos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tx_id TEXT,
                output_index INTEGER,
                address TEXT,
                amount REAL,
                spent INTEGER DEFAULT 0
            )
            """
        )
        await self.conn.commit()

    async def load_utxos_from_db(self):
        """Load UTXOs from the database into the utxo_pool."""
        await self.cursor.execute("SELECT tx_id, output_index, address, amount, spent FROM utxos WHERE spent = 0")
        rows = await self.cursor.fetchall()
        for row in rows:
            utxo_id = f"{row['tx_id']}:{row['output_index']}"
            self.utxo_pool[utxo_id] = {
                "tx_id": row["tx_id"],
                "output_index": row["output_index"],
                "address": row["address"],
                "amount": row["amount"],
                "spent": row["spent"],
            }
        logger.info(f"Loaded {len(self.utxo_pool)} UTXOs from the database.")

    async def load_or_create_wallet(self):
        """Load existing wallet or create a new one with encryption."""
        wallet_file = "wallet.dat"

        if os.path.exists(wallet_file):
            # Load the encrypted private key
            try:
                with open(wallet_file, "rb") as f:
                    encrypted_data = f.read()

                # Extract the salt and the encrypted private key
                salt = encrypted_data[:16]
                iv = encrypted_data[16:32]
                encrypted_private_key = encrypted_data[32:]

                # Prompt the user for the passphrase
                passphrase = getpass.getpass("Enter your wallet passphrase: ")

                # Derive the key using PBKDF2HMAC
                kdf_instance = PBKDF2HMAC(
                    algorithm=hashes.SHA256(),
                    length=32,
                    salt=salt,
                    iterations=100000,
                    backend=default_backend()
                )
                key = kdf_instance.derive(passphrase.encode())

                # Decrypt the private key
                cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
                decryptor = cipher.decryptor()
                padded_private_key = decryptor.update(encrypted_private_key) + decryptor.finalize()

                # Remove padding
                unpadder = padding.PKCS7(128).unpadder()
                private_key_bytes = unpadder.update(padded_private_key) + unpadder.finalize()

                # Load the private key
                self.private_key = serialization.load_pem_private_key(
                    private_key_bytes,
                    password=None,
                    backend=default_backend()
                )
                logger.info("Wallet private key loaded successfully.")
            except (InvalidKey, ValueError) as e:
                if isinstance(e, InvalidKey):
                    logger.error("Incorrect passphrase or corrupted wallet.dat file.")
                elif isinstance(e, ValueError):
                    logger.error("Invalid padding: The provided passphrase may be incorrect or the wallet file is corrupted.")
                raise e
            except Exception as e:
                logger.error(f"Failed to load wallet private key: {e}")
                raise e
        else:
            # Create a new wallet
            self.private_key = ec.generate_private_key(ec.SECP256K1())
            passphrase = getpass.getpass("Create a passphrase for your new wallet: ")
            confirm_passphrase = getpass.getpass("Confirm your passphrase: ")

            if passphrase != confirm_passphrase:
                logger.error("Passphrases do not match. Wallet creation aborted.")
                raise ValueError("Passphrases do not match.")

            # Generate a random salt
            salt = os.urandom(16)
            # Derive the key using PBKDF2HMAC
            kdf_instance = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100000,
                backend=default_backend()
            )
            key = kdf_instance.derive(passphrase.encode())

            # Serialize the private key
            private_key_bytes = self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            # Pad the private key for AES block size
            padder = padding.PKCS7(128).padder()
            padded_private_key = padder.update(private_key_bytes) + padder.finalize()

            # Generate a random IV
            iv = os.urandom(16)

            # Encrypt the private key
            cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
            encryptor = cipher.encryptor()
            encrypted_private_key = encryptor.update(padded_private_key) + encryptor.finalize()

            # Write the salt, IV, and encrypted private key to wallet.dat
            with open(wallet_file, "wb") as f:
                f.write(salt + iv + encrypted_private_key)

            logger.info("New wallet created and private key saved securely.")

        # Generate wallet address from the public key
        public_key = self.private_key.public_key()
        wallet_address_bytes = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        # Use SHA-256 hash of the public key as the wallet address
        self.wallet_address = hashlib.sha256(wallet_address_bytes).hexdigest()
        logger.info(f"Wallet address: {self.wallet_address}")

    def get_wallet_info(self):
        """Return the wallet address and balance."""
        balance = self.get_balance(self.wallet_address)
        return {
            "address": self.wallet_address,
            "balance": balance
        }

    def get_balance(self, address):
        """Calculate the balance for a given address."""
        balance = 0.0
        for utxo in self.utxo_pool.values():
            if utxo["address"] == address and not utxo["spent"]:
                balance += utxo["amount"]
        return balance

    async def create_transaction(self, receiver_address, amount):
        """Create a new transaction from the wallet to the receiver."""
        sender_address = self.wallet_address
        sender_private_key = self.private_key

        # Gather UTXOs for the sender to cover the amount
        utxos = self.get_utxos_for_amount(sender_address, amount)
        if not utxos:
            logger.error("Insufficient funds for the transaction.")
            return None

        # Prepare transaction inputs and outputs
        inputs = []
        total_input_amount = 0.0
        for utxo in utxos:
            # Include the sender's public key in the input
            sender_public_key_pem = sender_private_key.public_key().public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            ).decode('utf-8')
            inputs.append({
                "tx_id": utxo["tx_id"],
                "output_index": utxo["output_index"],
                "public_key": sender_public_key_pem
            })
            total_input_amount += utxo["amount"]

        outputs = [
            {"address": receiver_address, "amount": amount},
        ]
        # Include change output if necessary
        change = total_input_amount - amount
        if change > 0:
            outputs.append({"address": sender_address, "amount": change})

        transaction = {
            "inputs": inputs,
            "outputs": outputs,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        # Calculate transaction hash
        transaction_data = json.dumps(transaction, sort_keys=True).encode()
        transaction_hash = hashlib.sha256(transaction_data).hexdigest()
        transaction["hash"] = transaction_hash

        # Sign the transaction
        signature = sender_private_key.sign(
            transaction_hash.encode(),
            ec.ECDSA(hashes.SHA256())
        )
        transaction["signature"] = signature.hex()

        # Add transaction to the transaction pool
        self.transaction_pool.append(transaction)
        logger.info(f"Transaction {transaction_hash} created and added to transaction pool.")

        # Broadcast the transaction to peers
        await self.peer.broadcast_transaction(transaction)

        return transaction

    def get_utxos_for_amount(self, address, amount):
        """Get a list of UTXOs for the given address that cover the amount."""
        utxos = []
        total = 0.0
        for utxo_id, utxo in self.utxo_pool.items():
            if utxo["address"] == address and utxo["spent"] == 0:
                utxos.append(utxo)
                total += utxo["amount"]
                if total >= amount:
                    break
        return utxos if total >= amount else None

    async def receive_transaction(self, transaction):
        """Receive and process a transaction from a peer."""
        if await self.validate_transaction(transaction):
            self.transaction_pool.append(transaction)
            logger.info(f"Transaction {transaction['hash']} added to pool.")
        else:
            logger.error(f"Received invalid transaction {transaction.get('hash')}.")

    async def create_coinbase_transaction(self):
        """Create a coinbase transaction awarding the node."""
        # Use the node's own wallet address for the reward
        reward_address = self.wallet_address
        transaction = {
            "inputs": [],
            "outputs": [{"address": reward_address, "amount": 24.0}],
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signature": "",
        }

        # Calculate transaction hash
        transaction_data = json.dumps(transaction, sort_keys=True).encode()
        transaction_hash = hashlib.sha256(transaction_data).hexdigest()
        transaction["hash"] = transaction_hash

        # No signature required for coinbase transactions

        # Add transaction to the transaction pool
        self.transaction_pool.append(transaction)
        logger.info("Coinbase transaction created and added to transaction pool.")

        # Broadcast the transaction to peers
        await self.peer.broadcast_transaction(transaction)

        # Optionally, generate a new block immediately
        await self.generate_block()

    async def generate_block(self):
        """Generate a block with all pending transactions."""
        # Collect transactions to include in the block
        transactions = self.transaction_pool.copy()

        if not transactions:
            logger.info("No transactions to include in the block.")
            return

        # Get the previous block hash
        await self.cursor.execute("SELECT block_hash FROM blocks ORDER BY id DESC LIMIT 1")
        result = await self.cursor.fetchone()
        previous_hash = result["block_hash"] if result else "0" * 64  # Genesis block if no previous hash

        # Create the new block
        block = {
            "previous_hash": previous_hash,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "transactions": transactions,
        }

        # Calculate block hash
        block_hash = self.calculate_hash(block)
        block["hash"] = block_hash

        # Add the block to the blockchain
        await self.add_block(block)

        # Clear the transaction pool
        self.transaction_pool.clear()

    async def add_block(self, block):
        """Add a new block to the blockchain."""
        if await self.validate_block(block):
            await self.save_block_to_db(block)
            self.update_utxo(block)
            logger.info(f"Block {block['hash']} added to the blockchain.")
        else:
            logger.error("Invalid block. Rejected.")

    async def validate_transaction(self, transaction):
        """Validate a transaction."""
        # Check for required fields
        required_fields = ["inputs", "outputs", "timestamp", "hash", "signature"]
        for field in required_fields:
            if field not in transaction:
                logger.error(f"Transaction is missing field: {field}")
                return False

        # Recalculate the transaction hash and compare
        transaction_copy = transaction.copy()
        transaction_hash = transaction_copy.pop("hash")
        transaction_data = json.dumps(transaction_copy, sort_keys=True).encode()
        calculated_hash = hashlib.sha256(transaction_data).hexdigest()
        if transaction_hash != calculated_hash:
            logger.error("Transaction hash mismatch.")
            return False

        # For coinbase transactions, no inputs or signatures are required
        if not transaction["inputs"]:
            return True

        # Validate signatures and UTXOs for each input
        for tx_input in transaction["inputs"]:
            utxo_id = f"{tx_input['tx_id']}:{tx_input['output_index']}"
            utxo = self.utxo_pool.get(utxo_id)
            if not utxo:
                logger.error(f"Referenced UTXO {utxo_id} does not exist.")
                return False
            if utxo["spent"]:
                logger.error(f"Referenced UTXO {utxo_id} has already been spent.")
                return False

            # Retrieve the sender's address from the UTXO
            utxo_address = utxo["address"]

            # The transaction input must include the sender's public key
            sender_public_key_pem = tx_input.get("public_key")
            if not sender_public_key_pem:
                logger.error("Sender's public key is missing in the transaction input.")
                return False

            # Verify that the public key hashes to the address in the UTXO
            try:
                sender_public_key = serialization.load_pem_public_key(sender_public_key_pem.encode())
            except Exception as e:
                logger.error(f"Failed to load sender's public key: {e}")
                return False

            derived_address = hashlib.sha256(
                sender_public_key.public_bytes(
                    encoding=serialization.Encoding.PEM,
                    format=serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            ).hexdigest()
            if derived_address != utxo_address:
                logger.error("Public key does not match the UTXO address.")
                return False

            # Verify the signature using the provided public key
            try:
                sender_public_key.verify(
                    bytes.fromhex(transaction["signature"]),
                    transaction_hash.encode(),
                    ec.ECDSA(hashes.SHA256())
                )
            except InvalidSignature:
                logger.error("Invalid signature for transaction.")
                return False
            except Exception as e:
                logger.error(f"Error verifying signature: {e}")
                return False

        return True

    async def validate_block(self, block):
        """Validate a block before adding it to the blockchain."""
        # Check for required fields
        required_fields = ["previous_hash", "timestamp", "transactions", "hash"]
        for field in required_fields:
            if field not in block:
                logger.error(f"Block is missing field: {field}")
                return False

        # Recalculate the block hash and compare
        block_copy = block.copy()
        block_hash = block_copy.pop("hash")
        calculated_hash = self.calculate_hash(block_copy)
        if block_hash != calculated_hash:
            logger.error("Block hash mismatch.")
            return False

        # Validate each transaction in the block
        for transaction in block["transactions"]:
            if not await self.validate_transaction(transaction):
                logger.error(f"Invalid transaction {transaction['hash']} in block.")
                return False

        return True

    async def save_block_to_db(self, block):
        """Save a validated block and its transactions to the SQLite database."""
        # Save block
        await self.cursor.execute(
            "INSERT INTO blocks (block_hash, previous_hash, timestamp) VALUES (?, ?, ?)",
            (block["hash"], block["previous_hash"], block["timestamp"])
        )

        # Save transactions
        for transaction in block["transactions"]:
            sender = transaction["inputs"][0]["tx_id"] if transaction["inputs"] else "coinbase"
            receiver = transaction["outputs"][0]["address"]
            amount = transaction["outputs"][0]["amount"]

            await self.cursor.execute(
                "INSERT INTO transactions (tx_id, block_hash, sender, receiver, amount, timestamp, signature) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    transaction["hash"],
                    block["hash"],
                    sender,
                    receiver,
                    amount,
                    transaction["timestamp"],
                    transaction["signature"]
                )
            )

            # Update UTXO pool
            if transaction["inputs"]:
                for tx_input in transaction["inputs"]:
                    # Mark UTXO as spent
                    utxo_id = f"{tx_input['tx_id']}:{tx_input['output_index']}"
                    await self.cursor.execute(
                        "UPDATE utxos SET spent = 1 WHERE tx_id = ? AND output_index = ?",
                        (tx_input["tx_id"], tx_input["output_index"])
                    )
                    if utxo_id in self.utxo_pool:
                        self.utxo_pool[utxo_id]["spent"] = 1

            # Add new UTXOs
            for index, output in enumerate(transaction["outputs"]):
                await self.cursor.execute(
                    "INSERT INTO utxos (tx_id, output_index, address, amount, spent) VALUES (?, ?, ?, ?, 0)",
                    (transaction["hash"], index, output["address"], output["amount"])
                )
                utxo_id = f"{transaction['hash']}:{index}"
                self.utxo_pool[utxo_id] = {
                    "tx_id": transaction["hash"],
                    "output_index": index,
                    "address": output["address"],
                    "amount": output["amount"],
                    "spent": 0
                }

        await self.conn.commit()
        logger.info(f"Block {block['hash']} and its transactions saved to the database.")

    def update_utxo(self, block):
        """Update the UTXO pool based on transactions in the block."""
        for transaction in block["transactions"]:
            # Mark inputs as spent
            if transaction["inputs"]:
                for tx_input in transaction["inputs"]:
                    utxo_id = f"{tx_input['tx_id']}:{tx_input['output_index']}"
                    if utxo_id in self.utxo_pool:
                        self.utxo_pool[utxo_id]["spent"] = 1

            # Add outputs as new UTXOs
            for index, output in enumerate(transaction["outputs"]):
                utxo_id = f"{transaction['hash']}:{index}"
                self.utxo_pool[utxo_id] = {
                    "tx_id": transaction["hash"],
                    "output_index": index,
                    "address": output["address"],
                    "amount": output["amount"],
                    "spent": 0
                }

    def calculate_hash(self, data):
        """Calculate SHA-256 hash of the given data."""
        data_string = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_string.encode()).hexdigest()

    async def start(self):
        """Start the Crypto module tasks."""
        await self.init_blockchain_db()
        await self.load_utxos_from_db()
        await self.load_or_create_wallet()

    async def shutdown(self):
        """Shut down the Crypto module."""
        self.shutdown_flag = True
        logger.info("Crypto shutdown initiated.")
        try:
            await self.conn.close()
            logger.info("Crypto database connection closed.")
        except Exception as e:
            logger.error(f"Error closing Crypto database: {e}", exc_info=True)
        logger.info("Crypto module shutdown complete.")
