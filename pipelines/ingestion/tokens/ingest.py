import pandas as pd
import sys
from pathlib import Path
from tqdm import tqdm
from web3 import Web3
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import requests
import json
from joblib import Parallel, delayed
import boto3

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.helpers.util import tqdm_joblib
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.s3 import *
from ingestion.helpers.cypher import merge_wallet_nodes
from ingestion.tokens.helpers.cypher import *
from ingestion.tokens.helpers.util import get_receive_transactions, get_sent_transactions

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

now = datetime.now()
cutoff = now - timedelta(days=180)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

SPLIT_SIZE = 20000

provider = "https://eth-mainnet.alchemyapi.io/v2/" + str(os.environ.get("ALCHEMY_API_KEY"))
w3 = Web3(Web3.HTTPProvider(provider))
ilatest = w3.eth.get_block("latest")["number"]


def iblock_near(
    tunix_s,
    left_block_tuple=(1, w3.eth.get_block(1).timestamp),
    right_block_tuple=(ilatest, w3.eth.get_block("latest").timestamp),
):
    left_block = left_block_tuple[0]
    right_block = right_block_tuple[0]
    left_timestamp = left_block_tuple[1]
    right_timestamp = right_block_tuple[1]

    if left_block == right_block:
        return left_block
    # Return the closer one, if we're already between blocks
    if left_block == right_block - 1 or tunix_s <= left_timestamp or tunix_s >= right_timestamp:
        return left_block if abs(tunix_s - left_block_tuple[1]) < abs(tunix_s - right_block_tuple[1]) else right_block

    # K is how far inbetween left and right we're expected to be
    k = (tunix_s - left_timestamp) / (right_timestamp - left_timestamp)
    # We bound, to ensure logarithmic time even when guesses aren't great
    k = min(max(k, 0.05), 0.95)
    # We get the expected block number from K
    expected_block = round(left_block + k * (right_block - left_block))
    # Make sure to make some progress
    expected_block = min(max(expected_block, left_block + 1), right_block - 1)

    # Get the actual timestamp for that block
    expected_block_timestamp = w3.eth.get_block(expected_block).timestamp

    # Adjust bound using our estimated block
    if expected_block_timestamp < tunix_s:
        left_block = expected_block
        left_timestamp = expected_block_timestamp
    elif expected_block_timestamp > tunix_s:
        right_block = expected_block
        right_timestamp = expected_block_timestamp
    else:
        # Return the perfect match
        return expected_block

    # Recurse using tightened bounds
    return iblock_near(tunix_s, (left_block, left_timestamp), (right_block, right_timestamp))


if __name__ == "__main__":

    df = pd.read_csv("ingestion/tokens/addresses.csv")
    print(f"Loaded {len(df)} addresses")

    address_list = df["w.address"].tolist()

    start_block = iblock_near(cutoff.timestamp())
    print(f"Start block: {start_block}")
    print(f"Latest block: {ilatest}")

    with tqdm_joblib(tqdm(desc="Getting Sent Transactions", total=len(address_list))) as progress_bar:
        all_trans = Parallel(n_jobs=-1)(
            delayed(get_sent_transactions)(address, start_block, provider) for address in address_list
        )

    sent_trans = [x for y in all_trans for x in y]
    print(f"Sent transactions: {len(sent_trans)}")

    # with open("ingestion/tokens/sent.json", "w") as f:
    #     json.dump(sent_trans, f)

    # with open("ingestion/tokens/sent.json", "r") as f:
    #     sent_trans = json.load(f)

    with tqdm_joblib(tqdm(desc="Getting Received Transactions", total=len(address_list))) as progress_bar:
        all_trans = Parallel(n_jobs=-1)(
            delayed(get_receive_transactions)(address, start_block, provider) for address in address_list
        )

    received_trans = [x for y in all_trans for x in y]
    print(f"Received transactions: {len(received_trans)}")

    # with open("ingestion/tokens/received.json", "w") as f:
    #     json.dump(received_trans, f)

    # with open("ingestion/tokens/received.json", "r") as f:
    #     received_trans = json.load(f)

    # Categorize outgoing transactions
    erc721_sent = []
    erc20_sent = []

    for entry in sent_trans:
        current_dict = {}
        current_dict["address"] = entry["from"]
        current_dict["txHash"] = entry["hash"]
        current_dict["tokenAddress"] = entry["rawContract"]["address"]

        if entry["category"] == "erc721":
            current_dict["tokenId"] = entry["tokenId"]
            erc721_sent.append(current_dict)

        else:
            current_dict["amount"] = entry["value"]
            erc20_sent.append(current_dict)

    print("Sent ERC721 Nodes: ", len(erc721_sent))
    print("Sent ERC20 Nodes: ", len(erc20_sent))

    # sent ERC721
    erc721_df = pd.DataFrame(erc721_sent)
    erc721_df.drop_duplicates(subset=["txHash"], inplace=True)

    list_erc_chunks = split_dataframe(erc721_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_erc_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/tokens/nodes/sent/erc721-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        merge_erc721_nodes(url, conn)
        merge_sent_relationships(url, conn)
        set_object_private(BUCKET, f"neo/tokens/nodes/sent/erc721-{idx * SPLIT_SIZE}.csv", resource)

    # sent ERC20
    erc20_df = pd.DataFrame(erc20_sent)
    erc20_df.drop_duplicates(subset=["txHash"], inplace=True)

    list_erc_chunks = split_dataframe(erc20_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_erc_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/tokens/nodes/sent/erc20-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        merge_erc20_nodes(url, conn)
        merge_sent_relationships(url, conn)
        set_object_private(BUCKET, f"neo/tokens/nodes/sent/erc20-{idx * SPLIT_SIZE}.csv", resource)

    # Categorize incoming transactions
    erc721_received = []
    erc20_received = []

    for entry in received_trans:
        current_dict = {}
        current_dict["address"] = entry["from"]
        current_dict["txHash"] = entry["hash"]
        current_dict["tokenAddress"] = entry["rawContract"]["address"]

        if entry["category"] == "erc721":
            current_dict["tokenId"] = entry["tokenId"]
            erc721_received.append(current_dict)

        else:
            current_dict["amount"] = entry["value"]
            erc20_received.append(current_dict)

    print("Received ERC721 Nodes: ", len(erc721_received))
    print("Received ERC20 Nodes: ", len(erc20_received))

    # sent ERC721
    erc721_df = pd.DataFrame(erc721_received)
    erc721_df.drop_duplicates(subset=["txHash"], inplace=True)

    list_erc_chunks = split_dataframe(erc721_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_erc_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/tokens/nodes/rec/erc721-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        merge_erc721_nodes(url, conn)
        merge_received_relationships(url, conn)
        set_object_private(BUCKET, f"neo/tokens/nodes/rec/erc721-{idx * SPLIT_SIZE}.csv", resource)

    # sent ERC20
    erc20_df = pd.DataFrame(erc721_received)
    erc20_df.drop_duplicates(subset=["txHash"], inplace=True)

    list_erc_chunks = split_dataframe(erc721_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_erc_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/tokens/nodes/rec/erc20-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        merge_erc20_nodes(url, conn)
        merge_received_relationships(url, conn)
        set_object_private(BUCKET, f"neo/tokens/nodes/rec/erc20-{idx * SPLIT_SIZE}.csv", resource)

    conn.close()
