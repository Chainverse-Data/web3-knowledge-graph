import contextlib
import joblib
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
from web3 import Web3


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
                {"multisig": address.lower(), "address": owner.lower(), "threshold": int(result["threshold"])}
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
