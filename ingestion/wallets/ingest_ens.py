from web3 import Web3
from dotenv import load_dotenv
import json
import pandas as pd
import sys
from pathlib import Path
import boto3
import os
import eth_utils
from tqdm import tqdm
from joblib import Parallel, delayed
import requests
from datetime import datetime, timedelta

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.wallets.helpers.cypher import *
from ingestion.wallets.helpers.util import *
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.cypher import merge_ens_nodes, merge_ens_relationships
from ingestion.helpers.util import tqdm_joblib

SPLIT_SIZE = 10000
now = datetime.now() - timedelta(days=20)  # adjust days as needed

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

alchemy_key = str(os.environ.get("ALCHEMY_API_KEY"))
provider = "https://eth-mainnet.alchemyapi.io/v2/" + alchemy_key
w3 = Web3(Web3.HTTPProvider(provider))


def run_pipeline(address_list):

    with tqdm_joblib(tqdm(desc="Getting ENS Data", total=len(address_list))) as progress_bar:
        ens_list = Parallel(n_jobs=-1)(delayed(get_ens)(address, provider, alchemy_key) for address in address_list)

    ens_list = [x for x in ens_list if x is not None]
    ens_list = [x for y in ens_list for x in y]
    print(f"{len(ens_list)} ENS records found")

    final_list = []

    for ens in ens_list:
        final_list.append(
            {
                "name": ens["name"],
                "owner": ens["owner"].lower(),
                "tokenId": ens["token_id"],
                "txHash": ens["trans"]["hash"].lower(),
                "contractAddress": ens["trans"]["rawContract"]["address"],
            }
        )

    # create ens nodes and relationships
    ens_df = pd.DataFrame(final_list)
    ens_df.drop_duplicates(subset=["txHash"], inplace=True)
    ens_df = ens_df[ens_df["name"].notna()]
    ens_df = ens_df[ens_df["name"] != ""]
    print("ENS nodes: ", len(ens_df))

    conn = ChainverseGraph(uri, username, password)

    list_ens_chunks = split_dataframe(ens_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_ens_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/wallets/nodes/ens/ens-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_ens_nodes(url, conn)
        merge_ens_relationships(url, conn)
        set_object_private(BUCKET, f"neo/wallets/nodes/ens/ens-{idx * SPLIT_SIZE}.csv", resource)

    address_dict = [{"address": add} for add in address_list]
    address_df = pd.DataFrame(address_dict)
    address_df.drop_duplicates(subset=["address"], inplace=True)

    url = write_df_to_s3(address_df, BUCKET, f"neo/wallets/nodes/addresses/addresses.csv", resource, s3)
    add_ens_property(url, conn)
    set_object_private(BUCKET, f"neo/wallets/nodes/addresses/addresses.csv", resource)


if __name__ == "__main__":

    conn = ChainverseGraph(uri, username, password)
    address_list = get_recent_wallets_with_alias(now, conn)
    print(len(address_list))

    for i in range(0, len(address_list), SPLIT_SIZE):
        run_pipeline(address_list[i : i + SPLIT_SIZE])

    conn.close()
