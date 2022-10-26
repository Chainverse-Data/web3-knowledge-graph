import requests
import json
import time

headers = {"Accept": "application/json", "Content-Type": "application/json"}


def get_sent_transactions(address, start_block, provider, retries=0):

    all_transactions = []
    if retries > 5:
        return all_transactions

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": hex(start_block),
                "toBlock": "latest",
                "category": ["erc20", "erc721"],
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": "0x3e8",
                "order": "desc",
                "fromAddress": str(address),
            }
        ],
    }
    response = requests.post(provider, json=payload, headers=headers)
    response = json.loads(response.text)
    if "result" not in response:
        time.sleep(1)
        return get_sent_transactions(address, start_block, provider, retries + 1)
    result = response["result"]

    while "pagekey" in result:
        all_transactions.extend(result["transfers"])
        payload["params"][0]["pagekey"] = result["pagekey"]
        response = requests.post(provider, json=payload, headers=headers)
        response = json.loads(response.text)
        if "result" not in response:
            time.sleep(1)
            return get_sent_transactions(address, start_block, provider, retries + 1)
        result = response["result"]

    all_transactions.extend(result["transfers"])

    return all_transactions


def get_receive_transactions(address, start_block, provider, retries=0):

    all_transactions = []

    payload = {
        "id": 1,
        "jsonrpc": "2.0",
        "method": "alchemy_getAssetTransfers",
        "params": [
            {
                "fromBlock": hex(start_block),
                "toBlock": "latest",
                "category": ["erc20", "erc721"],
                "withMetadata": False,
                "excludeZeroValue": True,
                "maxCount": "0x3e8",
                "order": "desc",
                "toAddress": str(address),
            }
        ],
    }
    response = requests.post(provider, json=payload, headers=headers)
    response = json.loads(response.text)
    if "result" not in response:
        time.sleep(1)
        return get_sent_transactions(address, start_block, provider, retries + 1)
    result = response["result"]

    while "pagekey" in result:
        all_transactions.extend(result["transfers"])
        payload["params"][0]["pagekey"] = result["pagekey"]
        response = requests.post(provider, json=payload, headers=headers)
        response = json.loads(response.text)
        if "result" not in response:
            time.sleep(1)
            return get_sent_transactions(address, start_block, provider, retries + 1)
        result = response["result"]

    all_transactions.extend(result["transfers"])

    return all_transactions
