import logging
import sys
import os
from pathlib import Path
import argparse
import requests
import json
import eth_utils
from web3 import Web3
from web3.logs import DISCARD
import warnings

alchemy_url = f"https://eth-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY']}"
w3 = Web3(Web3.HTTPProvider(alchemy_url))
logging.debug(f"Web3 is connected? {w3.isConnected()}")


def expand_path(string):
    if string:
        return Path(os.path.expandvars(string))
    else:
        return None

def get_size(obj, seen=None):
    """Recursively finds size of objects"""
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([get_size(v, seen) for v in obj.values()])
        size += sum([get_size(k, seen) for k in obj.keys()])
    elif hasattr(obj, '__dict__'):
        size += get_size(obj.__dict__, seen)
    elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([get_size(i, seen) for i in obj])
    return size

def str2bool(v):
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")

def get_smart_contract(address):
    abi_endpoint = f"https://api.etherscan.io/api?module=contract&action=getabi&address={address}&apikey={os.environ['ETHERSCAN_API_KEY']}"
    abi = json.loads(requests.get(abi_endpoint).text)
    contract = w3.eth.contract(address, abi=abi["result"])
    return contract


def parse_logs(contract, tx_hash, name):
    receipt = w3.eth.get_transaction_receipt(tx_hash)
    event = [abi for abi in contract.abi if abi["type"] == "event" and abi["name"] == name][0]
    decoded_logs = contract.events[event["name"]]().processReceipt(receipt, errors=DISCARD)
    return decoded_logs

def get_ens_info(name):
    warnings.filterwarnings("ignore", category=FutureWarning)

    w3 = Web3(Web3.HTTPProvider(alchemy_url))
    try:
        owner = w3.ens.address(name=name).lower()

        x = eth_utils.crypto.keccak(eth_utils.conversions.to_bytes(text=name.replace(".eth", "")))
        token_id = eth_utils.conversions.to_int(x)

        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": "alchemy_getAssetTransfers",
            "params": [
                {
                    "fromBlock": "0x0",
                    "toBlock": "latest",
                    "contractAddresses": ["0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85"],
                    "category": ["erc721"],
                    "withMetadata": True,
                    "excludeZeroValue": True,
                    "maxCount": "0x3e8",
                    "order": "desc",
                    "toAddress": owner,
                }
            ],
        }

        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        response = requests.post(alchemy_url, json=payload, headers=headers, verify=False)
        x = json.loads(response.text)
        transfers = x["result"]["transfers"]
        flag = False
        trans = None
        for trans in transfers:
            if str(eth_utils.conversions.to_int(hexstr=trans["erc721TokenId"])) == str(token_id):
                flag = True
                trans = trans
                break

        if flag:
            ens_dict = {"name": name, "address": owner, "token_id": str(token_id), "trans": trans}
            return ens_dict
        else:
            return None
    except:
        return None
