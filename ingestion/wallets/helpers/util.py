import contextlib
import joblib
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from web3 import Web3
import eth_utils
import requests
import json


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


def query_gnosis_multisig(address):
    transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/gjeanmart/gnosis-safe-mainnet")
    client = Client(transport=transport, fetch_schema_from_transport=True)

    multisig_list = []
    query = gql(
        f""" {{
                        wallet(id: "{address}") {{
                            id
                            creator
                            network
                            stamp
                            hash
                            factory
                            mastercopy
                            owners
                            threshold
                            currentNonce
                            version
                        }}
                    }}"""
    )

    result = client.execute(query)
    result = result["wallet"]
    if result is not None:
        for owner in result["owners"]:
            multisig_list.append(
                {
                    "multisig": address.lower(),
                    "address": owner.lower(),
                    "threshold": int(result["threshold"]),
                    "occurDt": int(result["stamp"]),
                }
            )

    return multisig_list


def get_multisig_for_address(address):
    transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/gjeanmart/gnosis-safe-mainnet")
    client = Client(transport=transport, fetch_schema_from_transport=True)

    multisig_list = []
    try:
        query = gql(
            f"""{{
                    wallets(where: {{owners_contains: ["{address}"]}}) {{
                        id
                        creator
                        network
                        stamp
                        hash
                        owners
                        threshold
                    }}
                }}"""
        )

        result = client.execute(query)
        result = result["wallets"]
    except:
        result = []

    for entry in result:
        for owner in entry["owners"]:
            multisig_list.append(
                {
                    "multisig": entry["id"].lower(),
                    "address": owner.lower(),
                    "threshold": int(entry["threshold"]),
                    "occurDt": int(entry["stamp"]),
                }
            )

    return multisig_list


def categorize_wallet(address, w3):
    address = address.lower()
    x = w3.toChecksumAddress(address)

    # get address type (contract vs EOA)
    try:
        result = w3.eth.get_code(x).hex()
        if result == "0x":
            return {"address": address, "type": "EOA"}
        else:
            return {"address": address, "type": "contract"}
    except:
        pass


def get_ens(address, provider, key):

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    try:
        url = f"https://eth-mainnet.g.alchemy.com/nft/v2/{key}/getNFTs?owner={address}&contractAddresses[]=0x57f1887a8BF19b14fC0dF6Fd9B2acc9Af147eA85&withMetadata=true"
        response = requests.get(url, headers=headers)
        response = json.loads(response.text)

        all_ens = []
        for entry in response["ownedNfts"]:
            all_ens.append({"name": entry["title"], "token_id": int(entry["id"]["tokenId"], base=16)})

        if len(all_ens) == 0:
            return None

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
                    "toAddress": address,
                }
            ],
        }

        response = requests.post(provider, json=payload, headers=headers)
        x = json.loads(response.text)
        transfers = x["result"]["transfers"]
        final_list = []

        for entry in all_ens:
            for trans in transfers:
                if str(eth_utils.conversions.to_int(hexstr=trans["erc721TokenId"])) == str(entry["token_id"]):
                    final_list.append(
                        {
                            "name": entry["name"],
                            "owner": address.lower(),
                            "token_id": str(entry["token_id"]),
                            "trans": trans,
                        }
                    )
                    break

        return final_list
    except:
        return None
