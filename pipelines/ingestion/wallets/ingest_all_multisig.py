from re import sub
from web3 import Web3
from dotenv import load_dotenv
import pandas as pd
import sys
from pathlib import Path
import boto3
import os
from tqdm import tqdm
from joblib import Parallel, delayed
from datetime import datetime, timedelta
import math
import json

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.wallets.helpers.cypher import *
from ingestion.wallets.helpers.util import *
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.cypher import merge_wallet_nodes

SPLIT_SIZE = 10000
now = datetime.now() - timedelta(days=30)  # adjust days as needed

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

graph_url = f'https://gateway.thegraph.com/api/{os.environ.get("GRAPH_API_KEY")}/subgraphs/id/3oPKQiPKyD1obYpi5zXBy6HoPdYoDgxXptKrZ8GC3N1N'
total_entries = 100000
interval = 1000
jobs = 20

if __name__ == "__main__":

    all_starts = [x for x in range(0, total_entries, interval)]
    all_starts.append(total_entries + interval)

    splits = [i for i in range(0, len(all_starts) + 1, len(all_starts) // jobs)]
    if len(splits) == jobs + 1:
        splits[-1] = len(all_starts) - 1
    y = [(all_starts[splits[i]], all_starts[splits[i + 1]]) for i in range(len(splits) - 1)]
    assert len(y) == jobs

    with tqdm_joblib(tqdm(desc="Finding All Multisig Wallets", total=jobs)) as progress_bar:
        multi_raw = Parallel(n_jobs=jobs)(delayed(get_all_gnosis_multisig)(x[0], x[1], interval, graph_url) for x in y)

    multi_raw = [x for y in multi_raw for x in y]
    df = pd.DataFrame(multi_raw)
    address_list = df["address"].tolist() + df["multisig"].tolist()
    address_list = list(set(address_list))

    wallet_df = pd.DataFrame(address_list, columns=["address"])

    list_wallet_chunks = split_dataframe(wallet_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_wallet_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/wallet-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/wallets/wallet-{idx * SPLIT_SIZE}.csv", resource)

    raw_df = pd.DataFrame(address_list, columns=["address"])
    list_wallet_chunks = split_dataframe(raw_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_wallet_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/raw-wallet-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_multisig_property(url, conn)
        set_object_private(BUCKET, f"neo/wallets/raw-wallet-{idx * SPLIT_SIZE}.csv", resource)

    # add multisig labels
    multisig_df = df.drop_duplicates(subset=["multisig"])

    list_multisig_chunks = split_dataframe(multisig_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_multisig_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/multisig-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_multisig_labels(url, conn)
        set_object_private(BUCKET, f"neo/wallets/multisig-{idx * SPLIT_SIZE}.csv", resource)

    # add signer relationships
    df = df.drop_duplicates(subset=["address", "multisig"])

    list_signer_chunks = split_dataframe(df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_signer_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/signer-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_signer_relationships(url, conn)
        set_object_private(BUCKET, f"neo/wallets/signer-{idx * SPLIT_SIZE}.csv", resource)
