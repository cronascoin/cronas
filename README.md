# Cronas Server

## This is the Cronas Coin full node wallet.

This project is being written from scratch in python to be the best crypto currency out there. Currently we are developing the app.py to load an IRC type main program that will operate the network, network.py and we are developing the rpc.py program to handle rpc commands. The wallet uses two ports 4333 for network protocol and 4334 for RPC protocol.

The security model that is being proposed here is for a quantum resistant Master Key that is derived from a 12 word mnemonic. Users either use a randomly created list of words, or supply their own words. This is the heart of the keys for Cronas Coin as all other addresses are derived from the Master Key.

Our proposed system will start with a master key which is a sha3-512 hash of the mnemonic list of words.

The masterkey is then hashed again with a sha3-512 digest to produce two child keys by splitting the digest into 2 256 bit words.

The master key is split into two 256 bit words creating a left and right side. The right side is added to the left child key and if it is out of the limit of the sha-256, then the limit is subtracted from it. And the same thing is done to the other side. the left side of the master key is added to the right side child key in an x formation. Again the key is checked to make sure it is within the sha-256 limit. The result is you have two child private keys that can then be made into public keys.
Each of the child keys go through the same process and you get 4 more child private keys on the next level, 8 on the next level and so on. In this way you can create a tree of addresses that run indefinitely.

The reason we add the master key to the child key is that if we just hash them and if any key were compromised, every child key thereafter would be compromised by simply hashing them. It is presumed the master key is secure and therefore all child keys will be protected.

Developers work in their own trees.

## Installation
This software requires python 3.7 or higher. To check it:
```
python3 --version
```
if you are using a lower version, please upgrade it.
```
apt install python-pip
```
```
apt udate && apt upgrade -y
```
```
pip install aiohttp
```
```
git clone https://github.com/cronascoin/cronas.git
```
```
cd cronas
python3 app.py
```

Currently the project is in development.

## Usage

Instructions on how to use the project.

## Contributing

Guidelines for contributing to the project.

## License

The license under which the project is released.

