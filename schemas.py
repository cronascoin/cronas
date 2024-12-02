# schemas.py

blockchain_chunk_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "blockchain_chunk"},
        "data": {
            "type": "object",
            "properties": {
                "chunk": {"type": "string"},
                "start_byte": {"type": "integer"},
                "end_byte": {"type": "integer"},
                "is_last_chunk": {"type": "boolean"},
                "checksum": {"type": "string"}
            },
            "required": ["chunk", "start_byte", "end_byte", "is_last_chunk", "checksum"]
        }
    },
    "required": ["type", "data"]
}

getblockchain_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "getblockchain"},
        "data": {
            "type": "object",
            "properties": {
                "start_byte": {"type": "integer"},
                "chunk_size": {"type": "integer"}
            },
            "required": ["start_byte", "chunk_size"]
        }
    },
    "required": ["type", "data"]
}

utxo_data_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "utxo_data"},
        "data": {
            "type": "object",
            "properties": {
                "utxos": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "txid": {"type": "string"},
                            "vout": {"type": "integer"},
                            "address": {"type": "string"},
                            "amount": {"type": "number"}
                        },
                        "required": ["txid", "vout", "address", "amount"]
                    }
                }
            },
            "required": ["utxos"]
        }
    },
    "required": ["type", "data"]
}

resend_chunk_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "resend_chunk"},
        "data": {
            "type": "object",
            "properties": {
                "start_byte": {"type": "integer"},
                "chunk_size": {"type": "integer"}
            },
            "required": ["start_byte", "chunk_size"]
        }
    },
    "required": ["type", "data"]
}

error_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "error"},
        "data": {
            "type": "object",
            "properties": {
                "message": {"type": "string"}
            },
            "required": ["message"]
        }
    },
    "required": ["type", "data"]
}

# --- Newly Defined Schemas ---

heartbeat_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "heartbeat"},
        "data": {
            "type": "object",
            "properties": {
                "server_id": {"type": "string"},
                "uptime": {"type": "integer"},
                "timestamp": {"type": "number"}
            },
            "required": ["server_id", "uptime", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

heartbeat_ack_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "heartbeat_ack"},
        "data": {
            "type": "object",
            "properties": {
                "server_id": {"type": "string"},
                "timestamp": {"type": "number"}
            },
            "required": ["server_id", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

hello_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "hello"},
        "version": {"type": "string"},
        "server_id": {"type": "string"},
        "timestamp": {"type": "number"},
        "public_key": {"type": "string"},
        "signature": {"type": "string"}
    },
    "required": ["type", "version", "server_id", "timestamp", "public_key", "signature"]
}

ack_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "ack"},
        "version": {"type": "string"},
        "server_id": {"type": "string"},
        "timestamp": {"type": "number"},
        "signature": {"type": "string"}
    },
    "required": ["type", "version", "server_id", "timestamp", "signature"]
}

broadcast_message_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "broadcast_message"},
        "data": {
            "type": "object",
            "properties": {
                "message": {"type": "string"},
                "sender_id": {"type": "string"},
                "timestamp": {"type": "number"}
            },
            "required": ["message", "sender_id", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

direct_message_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "direct_message"},
        "data": {
            "type": "object",
            "properties": {
                "message": {"type": "string"},
                "sender_id": {"type": "string"},
                "recipient_id": {"type": "string"},
                "timestamp": {"type": "number"}
            },
            "required": ["message", "sender_id", "recipient_id", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

transaction_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "transaction"},
        "data": {
            "type": "object",
            "properties": {
                "txid": {"type": "string"},
                "inputs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "txid": {"type": "string"},
                            "vout": {"type": "integer"},
                            "scriptSig": {"type": "string"}
                        },
                        "required": ["txid", "vout", "scriptSig"]
                    }
                },
                "outputs": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "address": {"type": "string"},
                            "amount": {"type": "number"}
                        },
                        "required": ["address", "amount"]
                    }
                },
                "timestamp": {"type": "number"}
            },
            "required": ["txid", "inputs", "outputs", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

daily_block_request_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "daily_block_request"},
        "data": {
            "type": "object",
            "properties": {
                "server_id": {"type": "string"},
                "timestamp": {"type": "number"}
            },
            "required": ["server_id", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

daily_block_response_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "daily_block_response"},
        "data": {
            "type": "object",
            "properties": {
                "server_id": {"type": "string"},
                "verified": {"type": "boolean"}
            },
            "required": ["server_id", "verified"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

block_generation_request_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "block_generation_request"},
        "data": {
            "type": "object",
            "properties": {
                "server_id": {"type": "string"},
                "block_data": {"type": "object"}  # Define block structure as needed
            },
            "required": ["server_id", "block_data"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

block_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "block"},
        "data": {
            "type": "object",
            "properties": {
                "block_hash": {"type": "string"},
                "data": {"type": "object"},
                "timestamp": {"type": "number"}
            },
            "required": ["block_hash", "data", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

# Add revocation_schema
revocation_schema = {
    "type": "object",
    "properties": {
        "type": {"type": "string", "const": "revocation"},
        "data": {
            "type": "object",
            "properties": {
                "server_id": {"type": "string"},
                "reason": {"type": "string"},
                "timestamp": {"type": "number"}
            },
            "required": ["server_id", "reason", "timestamp"]
        },
        "signature": {"type": "string"}
    },
    "required": ["type", "data", "signature"]
}

# Mapping of all schemas by their type (if you have this)
schemas = {
    "blockchain_chunk": blockchain_chunk_schema,
    "getblockchain": getblockchain_schema,
    "utxo_data": utxo_data_schema,
    "resend_chunk": resend_chunk_schema,
    "error": error_schema,
    "heartbeat": heartbeat_schema,
    "heartbeat_ack": heartbeat_ack_schema,
    "hello": hello_schema,
    "ack": ack_schema,
    "broadcast_message": broadcast_message_schema,
    "direct_message": direct_message_schema,
    "transaction": transaction_schema,
    "daily_block_request": daily_block_request_schema,
    "daily_block_response": daily_block_response_schema,
    "block_generation_request": block_generation_request_schema,
    "block": block_schema,
    "revocation": revocation_schema,  # Added revocation_schema
    # Add other schemas as needed
}
