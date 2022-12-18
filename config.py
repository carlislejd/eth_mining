from web3 import Web3, HTTPProvider

from os import getenv
from dotenv import load_dotenv

load_dotenv()

def client():
    return Web3(HTTPProvider(getenv('NODE')))