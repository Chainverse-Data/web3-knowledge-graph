from pandas.core.indexes import multi
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

provider = "https://eth-mainnet.alchemyapi.io/v2/" + str(os.environ.get("ALCHEMY_API_KEY"))
w3 = Web3(Web3.HTTPProvider(provider))

if __name__ == "__main__":

    # df = pd.read_csv("ingestion/tokens/addresses.csv")
    # print(f"Loaded {len(df)} addresses")
    # address_list = df["w.address"].tolist()
    # address_list = address_list[:50]
    address_list = get_recent_wallets_no_multsig(now, conn)
    print(f"Loaded {len(address_list)} addresses")

    # fetch multsig wallets
    with tqdm_joblib(tqdm(desc="Finding Multisig Wallets", total=len(address_list))) as progress_bar:
        multi_raw = Parallel(n_jobs=-1)(delayed(get_multisig_for_address)(address) for address in address_list)
    multi_raw = [x for y in multi_raw for x in y]
    print(f"Found {len(multi_raw)} MultiSig Wallets")

    df = pd.DataFrame(multi_raw)
    # df.to_csv("ingestion/wallets/multisig.csv", index=False)

    # get all wallets and multisig addresses and add to graph
    all_addresses = df["address"].tolist() + df["multisig"].tolist()
    raw_add = list(set(all_addresses))
    all_addresses = list(set(all_addresses) - set(address_list))
    print(f"Found {len(all_addresses)} unique addresses")

    wallet_df = pd.DataFrame(all_addresses, columns=["address"])

    list_wallet_chunks = split_dataframe(wallet_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_wallet_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/wallet-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/wallets/wallet-{idx * SPLIT_SIZE}.csv", resource)

    raw_df = pd.DataFrame(list(set(address_list).union(set(df["multisig"].tolist()))), columns=["address"])
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
