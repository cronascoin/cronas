from datetime import datetime

class Transaction:
    def __init__(self, sender, receiver, amount):
        self.sender = sender
        self.receiver = receiver
        self.amount = amount
        self.timestamp = datetime.now()

    def to_dict(self):
        return {
            'sender': self.sender,
            'receiver': self.receiver,
            'amount': self.amount,
            'timestamp': self.timestamp.isoformat()
        }

class Block:
    def __init__(self, transactions, previous_hash=''):
        self.transactions = transactions
        self.timestamp = datetime.now()
        self.previous_hash = previous_hash
        # Placeholder for the hash of this block
        self.hash = self.calculate_hash()

    def calculate_hash(self):
        # This is a simplified placeholder. In practice, you would use a cryptographic hash function.
        block_contents = str(self.timestamp) + str(self.transactions) + self.previous_hash
        return str(hash(block_contents))

    def to_dict(self):
        return {
            'transactions': [tx.to_dict() for tx in self.transactions],
            'timestamp': self.timestamp.isoformat(),
            'previous_hash': self.previous_hash,
            'hash': self.hash
        }
