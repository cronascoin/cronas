# Cronas Server

## This is the Cronas Coin full node wallet.

This project is being written from scratch in python to be the best crypto currency out there. Currently we are working on the peer to peer part of the program. The wallet uses two ports 4333 for network protocol and 4334 for RPC protocol. If you have suggestions or want to help please feel free to do so.

Developers work in their own trees.

## Installation
This software requires python 3.7 or higher. To check it:
```
python3 --version
```
if you are using a lower version, please upgrade it.
```
apt install pip
```
or 
```
sudo apt install python3-pip
```
```
apt update && apt upgrade -y
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

For a list of peers
```
python3 cli.py peers
```
To add nodes
```
python3 cli.py addnode <ip address>
```


## Contributing

Guidelines for contributing to the project.

## License

The license under which the project is released.

