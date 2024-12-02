# ssl_utils.py

import ssl
import logging

logger = logging.getLogger(__name__)

def create_server_ssl_context(certfile, keyfile, ca_certfile=None, require_client_cert=False):
    """
    Creates an SSL context for server operations.

    Args:
        certfile (str): Path to the server's certificate file.
        keyfile (str): Path to the server's private key file.
        ca_certfile (str, optional): Path to the CA certificate file for client verification.
        require_client_cert (bool, optional): Whether to require client certificates (for mTLS).

    Returns:
        ssl.SSLContext: Configured server SSL context.
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=certfile, keyfile=keyfile)

    if ca_certfile:
        context.load_verify_locations(cafile=ca_certfile)
        context.verify_mode = ssl.CERT_REQUIRED if require_client_cert else ssl.CERT_OPTIONAL
        logger.info(
            f"Server SSL context configured with {'required' if require_client_cert else 'optional'} client certificates."
        )
    else:
        context.verify_mode = ssl.CERT_NONE
        logger.warning("CA certificate file not provided. Client certificate verification disabled.")

    # Optional: Disable older TLS protocols
    context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    logger.info("Server SSL context created successfully.")
    return context

def create_client_ssl_context(certfile=None, keyfile=None, ca_certfile=None):
    """
    Creates an SSL context for client operations, allowing self-signed certificates.

    Args:
        certfile (str, optional): Path to the client's certificate file (for mTLS).
        keyfile (str, optional): Path to the client's private key file (for mTLS).
        ca_certfile (str, optional): Path to the CA certificate file for server verification.

    Returns:
        ssl.SSLContext: Configured client SSL context.
    """
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    
    if ca_certfile:
        context.load_verify_locations(cafile=ca_certfile)
        logger.info("Client SSL context loaded with CA certificate.")
    else:
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE  # Accept self-signed certificates
        logger.warning("CA certificate file not provided. Verification disabled for self-signed certificates.")

    if certfile and keyfile:
        context.load_cert_chain(certfile=certfile, keyfile=keyfile)
        logger.info("Client SSL context loaded with client certificate and key for mTLS.")
    else:
        logger.info("Client SSL context loaded without client certificate and key.")

    # Optional: Disable older TLS protocols
    context.options |= ssl.OP_NO_TLSv1 | ssl.OP_NO_TLSv1_1
    logger.info("Client SSL context created successfully.")
    return context
