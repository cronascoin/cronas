# crypto.py

import asyncio
import json
import logging
import os
import time
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography import x509
from cryptography.x509.oid import NameOID
import datetime
import ssl
import hashlib

logger = logging.getLogger(__name__)

class Crypto:
    def __init__(self, blockchain_file, utxo_file, wallet_file, certfile, keyfile, trusted_certs_dir='trusted_certs'):
        self.blockchain_file = blockchain_file
        self.utxo_file = utxo_file
        self.wallet_file = wallet_file
        self.certfile = certfile
        self.keyfile = keyfile
        self.trusted_certs_dir = trusted_certs_dir

        self.ssl_server_context = None  # Will be initialized in start()
        self.ssl_client_context = None  # Will be initialized in start()

        self.lock = asyncio.Lock()
        self.blockchain = []
        self.utxo_pool = []
        self.address = ""
        self.private_key = None
        self.public_key = None
        self.peer_public_keys = {}  # Mapping from server_id to public_key object

        # Ensure the trusted_certs_dir exists
        if not os.path.exists(self.trusted_certs_dir):
            os.makedirs(self.trusted_certs_dir)
            logger.info(f"Created trusted certificates directory at {self.trusted_certs_dir}")

    # ----------------------------
    # SSL Certificate Management
    # ----------------------------

    def create_server_ssl_context(self, certfile: str, keyfile: str, require_client_cert: bool = False) -> ssl.SSLContext:
        """
        Creates an SSL context for the server.

        Args:
            certfile (str): Path to the server's certificate file.
            keyfile (str): Path to the server's private key file.
            require_client_cert (bool, optional): Whether to require client certificates. Defaults to False.

        Returns:
            ssl.SSLContext: Configured SSL context for the server.
        """
        context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE  # Initially, no verification
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)

        if require_client_cert:
            self._configure_verify_client_certs(context)
        return context

    def create_client_ssl_context(self, certfile: str = None, keyfile: str = None) -> ssl.SSLContext:
        """
        Creates an SSL context for the client.

        Args:
            certfile (str, optional): Path to the client's certificate file. Defaults to None.
            keyfile (str, optional): Path to the client's private key file. Defaults to None.

        Returns:
            ssl.SSLContext: Configured SSL context for the client.
        """
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = False
        self._configure_verify_server_certs(context)
        if certfile and keyfile:
            if os.path.isfile(certfile) and os.path.isfile(keyfile):
                context.load_cert_chain(certfile=certfile, keyfile=keyfile)
                logger.info("Client certificate loaded.")
            else:
                logger.warning("Client certificate or key file not found. Client certificate loading skipped.")
        elif certfile or keyfile:
            logger.warning("Incomplete client certificate information provided. Client certificate loading skipped.")

        return context

    def _configure_verify_client_certs(self, context):
        """
        Configures the SSL context to verify client certificates using the trusted certificates directory.

        Args:
            context (ssl.SSLContext): The SSL context to configure.
        """
        self._extracted_from__configure_verify_server_certs_8(
            context,
            "Client certificate verification enabled using trusted_certs directory.",
        )

    def _configure_verify_server_certs(self, context):
        """
        Configures the SSL context to verify server certificates using the trusted certificates directory.

        Args:
            context (ssl.SSLContext): The SSL context to configure.
        """
        self._extracted_from__configure_verify_server_certs_8(
            context,
            "Server certificate verification enabled using trusted_certs directory.",
        )

    # TODO Rename this here and in `_configure_verify_client_certs` and `_configure_verify_server_certs`
    def _extracted_from__configure_verify_server_certs_8(self, context, arg1):
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(capath=self.trusted_certs_dir)
        logger.info(arg1)

    def generate_self_signed_certificates(self):
        """
        Generates self-signed SSL certificates if they don't exist.
        """
        if os.path.isfile(self.certfile) and os.path.isfile(self.keyfile):
            logger.info("SSL certificate and key already exist. Skipping generation.")
            return

        logger.info("Generating self-signed SSL certificate and private key...")

        # Generate private key
        self.private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )

        # Write private key to file
        with open(self.keyfile, "wb") as key_file:
            key_file.write(self.private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.TraditionalOpenSSL,  # PKCS#1
                encryption_algorithm=serialization.NoEncryption()
            ))
        logger.info(f"Private key saved to {self.keyfile}")

        # Generate self-signed certificate
        subject = issuer = x509.Name([
            x509.NameAttribute(NameOID.COUNTRY_NAME, u"US"),  # Update as needed
            x509.NameAttribute(NameOID.STATE_OR_PROVINCE_NAME, u"California"),  # Update as needed
            x509.NameAttribute(NameOID.LOCALITY_NAME, u"San Francisco"),  # Update as needed
            x509.NameAttribute(NameOID.ORGANIZATION_NAME, u"MyPeerOrganization"),  # Update as needed
            x509.NameAttribute(NameOID.COMMON_NAME, u"localhost"),  # Update as needed
        ])

        cert = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(issuer)
            .public_key(self.private_key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(datetime.datetime.now(datetime.timezone.utc))
            .not_valid_after(
                datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(days=365)
            )
            .add_extension(
                x509.SubjectAlternativeName([x509.DNSName(u"localhost")]),
                critical=False,
            )
            .sign(self.private_key, hashes.SHA256(), default_backend())
        )

        # Write certificate to file
        with open(self.certfile, "wb") as cert_file:
            cert_file.write(cert.public_bytes(serialization.Encoding.PEM))
        logger.info(f"Self-signed certificate saved to {self.certfile}")

    # ----------------------------
    # Trusted Certificates Management
    # ----------------------------

    def add_trusted_certificate(self, server_id: str, cert_pem: str):
        """
        Adds a peer's certificate to the trusted_certs directory and loads its public key.

        Args:
            server_id (str): Unique identifier for the server/peer.
            cert_pem (str): PEM-encoded certificate string.
        """
        cert_path = os.path.join(self.trusted_certs_dir, f"{server_id}.pem")
        if not os.path.isfile(cert_path):
            try:
                with open(cert_path, "w") as cert_file:
                    cert_file.write(cert_pem)
                logger.info(f"Added trusted certificate for server_id {server_id} at {cert_path}")
            except Exception as e:
                logger.error(f"Failed to add trusted certificate for server_id {server_id}: {e}")
        else:
            logger.info(f"Trusted certificate for server_id {server_id} already exists at {cert_path}")

        if public_key_pem := self.extract_public_key_from_certificate(cert_pem):
            self.add_peer_public_key(server_id, public_key_pem)
        else:
            logger.warning(f"Failed to extract public key from certificate for server_id {server_id}.")

    def get_trusted_certificate_path(self, server_id: str) -> str:
        """
        Retrieves the file path of a trusted certificate for a given server_id.

        Args:
            server_id (str): Unique identifier for the server/peer.

        Returns:
            str: Path to the trusted certificate file, or None if not found.
        """
        cert_path = os.path.join(self.trusted_certs_dir, f"{server_id}.pem")
        if os.path.isfile(cert_path):
            return cert_path
        logger.warning(f"Trusted certificate for server_id {server_id} not found.")
        return None

    def extract_public_key_from_certificate(self, cert_pem: str) -> str:
        """
        Extracts the public key from a PEM-encoded certificate.

        Args:
            cert_pem (str): PEM-encoded certificate string.

        Returns:
            str: PEM-encoded public key string, or None if extraction fails.
        """
        try:
            cert = x509.load_pem_x509_certificate(cert_pem.encode('utf-8'), backend=default_backend())
            public_key = cert.public_key()
            return public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo,
            ).decode('utf-8')
        except Exception as e:
            logger.error(f"Failed to extract public key from certificate: {e}")
            return None

    def add_peer_public_key(self, server_id: str, public_key_pem: str):
        """
        Adds a peer's public key to the peer_public_keys mapping.

        Args:
            server_id (str): Unique identifier for the server/peer.
            public_key_pem (str): PEM-encoded public key string.
        """
        try:
            public_key = serialization.load_pem_public_key(
                public_key_pem.encode('utf-8'),
                backend=default_backend()
            )
            self.peer_public_keys[server_id] = public_key
            logger.info(f"Added public key for server_id {server_id}")
        except Exception as e:
            logger.error(f"Failed to load public key for server_id {server_id}: {e}")

    def get_public_key(self, server_id: str):
        """
        Retrieve the public key of a peer by server_id.

        Args:
            server_id (str): The server ID of the peer.

        Returns:
            PublicKey object or None if not found.
        """
        return self.peer_public_keys.get(server_id)

    # ----------------------------
    # Blockchain, UTXO, and Wallet Management
    # ----------------------------

    def load_blockchain(self):
        """
        Load the blockchain from the specified file.
        Initializes a new blockchain if the file doesn't exist.
        """
        if os.path.exists(self.blockchain_file):
            with open(self.blockchain_file, 'r') as f:
                self.blockchain = json.load(f)
            logger.info(f"Loaded blockchain from {self.blockchain_file}")
        else:
            self.blockchain = []
            self._save_to_file(self.blockchain_file, self.blockchain)
            logger.info(f"Initialized new blockchain at {self.blockchain_file}")

    def load_utxos(self):
        """
        Load the UTXO pool from the specified file.
        Initializes an empty pool if the file doesn't exist.
        """
        if os.path.exists(self.utxo_file):
            with open(self.utxo_file, 'r') as f:
                self.utxo_pool = json.load(f)
            logger.info(f"Loaded UTXO pool from {self.utxo_file}")
        else:
            self.utxo_pool = []
            self._save_to_file(self.utxo_file, self.utxo_pool)
            logger.info(f"Initialized new UTXO pool at {self.utxo_file}")

    def load_wallet(self):
        """
        Load the wallet from the specified file.
        Creates a new wallet if the file doesn't exist.
        """
        if os.path.exists(self.wallet_file):
            try:
                self._extracted_from_load_wallet_8()
            except KeyError as e:
                logger.error(f"Missing key in wallet file: {e}")
                raise
            except json.JSONDecodeError as e:
                logger.error(f"Invalid JSON format in wallet file: {e}")
                raise
            except Exception as e:
                logger.error(f"Error loading wallet: {e}")
                raise
        else:
            self._create_new_wallet()

    # TODO Rename this here and in `load_wallet`
    def _extracted_from_load_wallet_8(self):
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

        Returns:
            str: PEM-encoded public key as a string.
        """
        public_key = self.private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode('utf-8')
        return public_key

    def generate_address(self) -> str:
        """
        Generates the wallet address using SHA256 of the public key.

        Returns:
            str: The generated address as a hex string.
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

        Args:
            wallet_data (dict): Wallet data to be saved.
        """
        self._save_to_file(self.wallet_file, wallet_data)

    @staticmethod
    def _save_to_file(filename: str, data):
        """
        Save data to a file in JSON format.

        Args:
            filename (str): Path to the file.
            data (dict or list): Data to save.
        """
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=4)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Failed to save data to {filename}: {e}")
            raise

    # ----------------------------
    # Transaction Management
    # ----------------------------

    async def create_transaction(self, receiver_address: str, amount: float) -> str:
        """
        Create a transaction with the specified receiver address and amount.

        Args:
            receiver_address (str): The address of the receiver.
            amount (float): The amount to send.

        Returns:
            str: The transaction ID (txid).

        Raises:
            ValueError: If insufficient funds are available.
        """
        async with self.lock:
            selected_utxos = []
            total = 0.0
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
                "inputs": [{"txid": utxo['txid'], "vout": utxo['vout']} for utxo in selected_utxos],
                "outputs": [
                    {"address": receiver_address, "amount": amount},
                    {"address": self.address, "amount": round(total - amount, 8)}  # Change
                ]
            }
            for utxo in selected_utxos:
                self.utxo_pool.remove(utxo)
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

    def generate_txid(self) -> str:
        """
        Generate a unique transaction ID.

        Returns:
            str: A unique transaction ID.
        """
        return f"tx{int(time.time() * 1000)}"

    # ----------------------------
    # Shutdown Handling
    # ----------------------------

    async def shutdown(self):
        """
        Perform any necessary cleanup during shutdown.
        """
        logger.info("Crypto module shutdown initiated.")
        # Add any cleanup tasks here if necessary
        logger.info("Crypto module shutdown complete.")

    # ----------------------------
    # Additional Cryptographic Operations
    # ----------------------------

    def sign(self, message: bytes) -> bytes:
        """
        Sign the message using the private key.

        Args:
            message (bytes): The message to sign.

        Returns:
            bytes: The signature.
        """
        if isinstance(self.private_key, rsa.RSAPrivateKey):
            return self.private_key.sign(
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                hashes.SHA256(),
            )
        logger.error("Private key is not RSA. Sign method only supports RSA keys.")
        raise TypeError("Private key must be RSA.")

    def verify(self, message: bytes, signature: bytes, server_id: str) -> bool:
        """
        Verify the signature of a message using the public key of the peer.

        Args:
            message (bytes): The original message.
            signature (bytes): The signature to verify.
            server_id (str): The server ID of the peer whose public key to use.

        Returns:
            bool: True if the signature is valid, False otherwise.
        """
        public_key = self.get_public_key(server_id)
        if not public_key:
            logger.error(f"No public key found for server_id {server_id}")
            return False
        try:
            public_key.verify(
                signature,
                message,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
            return True
        except Exception as e:
            logger.error(f"Signature verification failed for server_id {server_id}: {e}")
            return False

    # ----------------------------
    # Start Method
    # ----------------------------

    async def start(self):
        """
        Initialize the Crypto module by generating SSL certificates,
        creating SSL contexts, and loading blockchain, UTXO, and wallet data.
        """
        logger.info("Starting Crypto module...")
        
        # Generate self-signed SSL certificates if they don't exist
        self.generate_self_signed_certificates()

        # Initialize SSL contexts
        self.ssl_server_context = self.create_server_ssl_context(
            certfile=self.certfile,
            keyfile=self.keyfile,
            require_client_cert=True
        )
        self.ssl_client_context = self.create_client_ssl_context(
            certfile=None,
            keyfile=None
        )

        # Load blockchain, UTXO, and wallet data
        self.load_blockchain()
        self.load_utxos()
        self.load_wallet()

        logger.info("Crypto module started successfully.")

    def get_self_certificate_pem(self) -> str:
        """
        Retrieves the server's own certificate in PEM format.

        Returns:
            str: PEM-encoded certificate string, or empty string if not found.
        """
        if os.path.isfile(self.certfile):
            try:
                with open(self.certfile, 'rb') as f:
                    return f.read().decode('utf-8')
            except Exception as e:
                logger.error(f"Failed to read certificate file {self.certfile}: {e}")
                return ""
        else:
            logger.error(f"Certificate file {self.certfile} not found.")
            return ""

    # ----------------------------
    # Additional Utility Methods
    # ----------------------------

    async def update_trusted_certificates(self):
        """
        Periodically update trusted certificates by scanning the trusted_certs_dir.
        This can be useful if certificates are added dynamically.
        """
        while True:
            try:
                for cert_filename in os.listdir(self.trusted_certs_dir):
                    if cert_filename.endswith('.pem'):
                        server_id = cert_filename[:-4]  # Remove '.pem' extension
                        if server_id not in self.peer_public_keys:
                            cert_path = os.path.join(self.trusted_certs_dir, cert_filename)
                            with open(cert_path, 'r') as cert_file:
                                cert_pem = cert_file.read()
                            if public_key_pem := self.extract_public_key_from_certificate(
                                cert_pem
                            ):
                                self.add_peer_public_key(server_id, public_key_pem)
                await asyncio.sleep(300)  # Update every 5 minutes
            except Exception as e:
                logger.error(f"Error updating trusted certificates: {e}")
                await asyncio.sleep(300)  # Retry after delay

    # ----------------------------
    # Example Method Implementations
    # ----------------------------

    # These methods are referenced in message.py and should be implemented accordingly.
    # Depending on your application's architecture, these might belong to another class or module.

    def get_authority_ids(self) -> set:
        """
        Retrieves a set of authority server IDs that are trusted to send revocation messages.

        Returns:
            set: A set of trusted authority server IDs.
        """
        # Example implementation; replace with actual authority IDs management
        return {"authority_server_1", "authority_server_2"}

    def verify_block(self, block_data: dict) -> bool:
        """
        Verify the integrity and validity of a block.

        Args:
            block_data (dict): The block data to verify.

        Returns:
            bool: True if the block is valid, False otherwise.
        """
        # Implement block verification logic here
        # This is a placeholder implementation
        try:
            # Example: Check if block_hash matches the hash of block_data
            expected_hash = hashlib.sha256(json.dumps(block_data, sort_keys=True).encode('utf-8')).hexdigest()
            return block_data.get("block_hash") == expected_hash
        except Exception as e:
            logger.error(f"Block verification failed: {e}")
            return False

    async def reward_node(self, generator_address: str):
        """
        Reward the node that generated a valid block.

        Args:
            generator_address (str): The address of the node to reward.
        """
        # Implement reward logic here
        # This is a placeholder implementation
        reward_amount = 50.0  # Example reward
        await self.create_transaction(generator_address, reward_amount)
        logger.info(f"Rewarded node {generator_address} with {reward_amount} units.")

    async def broadcast_block(self, block: dict):
        """
        Broadcast a newly created block to all connected peers.

        Args:
            block (dict): The block data to broadcast.
        """
        # Implement block broadcasting logic here
        # This is a placeholder implementation
        for peer_id, peer_info in self.peer.active_peers.items():
            writer = peer_info['writer']
            try:
                await self.peer.message_handler.send_message(writer, block)
                logger.info(f"Broadcasted block {block.get('block_hash')} to peer {peer_id}")
            except Exception as e:
                logger.error(f"Failed to broadcast block to peer {peer_id}: {e}")

    async def schedule_rewrite(self):
        """
        Schedule a rewrite of the known_peers file.
        """
        # Implement peer list persistence logic here
        # This is a placeholder implementation
        await asyncio.sleep(0)  # Replace with actual file write logic
        logger.info("Scheduled rewrite of known_peers.")

    # ----------------------------
    # Initialization Tasks
    # ----------------------------

    async def initialize(self):
        """
        Initialize all necessary components for the Crypto module.
        """
        await self.start()
        asyncio.create_task(self.update_trusted_certificates())
