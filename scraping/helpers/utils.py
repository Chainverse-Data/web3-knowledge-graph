import logging
import contextlib
import joblib
import os
from pathlib import Path
import argparse
import requests
import json
from web3 import Web3
from web3.logs import DISCARD

alchemy_url = f"https://eth-mainnet.g.alchemy.com/v2/{os.environ['ALCHEMY_API_KEY']}"
w3 = Web3(Web3.HTTPProvider(alchemy_url))
logging.debug(f"Web3 is connected? {w3.isConnected()}")

@contextlib.contextmanager
def tqdm_joblib(tqdm_object):
    """Context manager to patch joblib to report into tqdm progress bar given as argument"""

    class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
        def __call__(self, *args, **kwargs):
            tqdm_object.update(n=self.batch_size)
            return super().__call__(*args, **kwargs)

    old_batch_callback = joblib.parallel.BatchCompletionCallBack
    joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
    try:
        yield tqdm_object
    finally:
        joblib.parallel.BatchCompletionCallBack = old_batch_callback
        tqdm_object.close()


def expand_path(string):
    if string:
        return Path(os.path.expandvars(string))
    else:
        return None


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