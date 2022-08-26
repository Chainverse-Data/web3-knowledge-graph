import argparse
from pathlib import Path
import os
import requests
import time
from web3 import Web3
import eth_utils
import json
import joblib
import contextlib

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


# Send POST request to API endpoint and return content/error message
def run_snapshot_query(q, retries=0):
    if retries > 10:
        return "[]"
    try:
        response = requests.post("https://hub.snapshot.org/graphql", "", json={"query": q})
        if response.status_code == 200:
            return response.text
        else:
            time.sleep(2)
            return run_snapshot_query(q, retries=retries + 1)
    except:
        print("Error in run_snapshot_query")
        time.sleep(2)
        return run_snapshot_query(q, retries=retries + 1)


def get_ens(space, provider):

    w3 = Web3(Web3.HTTPProvider(provider))
    name = space["id"]
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

        response = requests.post(provider, json=payload, headers=headers)
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
            ens_dict = {"owner": owner, "token_id": str(token_id), "trans": trans}
            return ens_dict
        else:
            return None
    except:
        return None
