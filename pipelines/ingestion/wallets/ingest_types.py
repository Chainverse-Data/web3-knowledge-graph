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
now = datetime.now() - timedelta(days=5)  # adjust days as needed

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

    address_list = get_recent_non_categorized_wallets(now, conn)
    print(len(address_list))
    # address_list = address_list[:1000]  # adjust number of addresses as needed
    # address_list = ["0x85ecca75f99aebc79e2a69540d95d9990c2aad2a", "0x4BB9E7D221fC35234a08cCdcb38D47D53Fb1973e"]

    # categorize wallets as EOA or contract
    with tqdm_joblib(tqdm(desc="Categorizing Wallets", total=len(address_list))) as progress_bar:
        fetched_adds = Parallel(n_jobs=-1)(delayed(categorize_wallet)(address, w3) for address in address_list)
    fetched_adds = [x for x in fetched_adds if x is not None]
    print(f"{len(fetched_adds)} wallets categorized")

    eoa_list = [x for x in fetched_adds if x["type"] == "EOA"]
    contract_list = [x for x in fetched_adds if x["type"] == "contract"]
    contract_addresses = [x["address"] for x in contract_list]
    eoa_addresses = [x["address"] for x in eoa_list]
    multisig_addresses = [x for x in address_list if x not in eoa_addresses]

    # fetch multsig wallets
    with tqdm_joblib(tqdm(desc="Querying MultiSig Wallets", total=len(multisig_addresses))) as progress_bar:
        multi_raw = Parallel(n_jobs=8)(delayed(query_gnosis_multisig)(address) for address in multisig_addresses)
    multi_raw = [x for x in multi_raw if len(x) > 0]
    print(f"Found {len(multi_raw)} MultiSig Wallets")

    multi_list = [x for y in multi_raw for x in y]
    multi_add_list = [x["multisig"] for x in multi_list]
    contract_list = [
        x for x in contract_list if x["address"] not in multi_add_list
    ]  # remove multisig wallets from contract list

    multi_df = pd.DataFrame(multi_list)

    # add multisig labels
    label_df = multi_df.drop_duplicates(subset=["multisig"], keep="first")
    label_df.reset_index(inplace=True, drop=True)

    list_multisig_chunks = split_dataframe(label_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_multisig_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/multi-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_multisig_labels(url, conn)
        set_object_private(BUCKET, f"neo/wallets/multi-{idx * SPLIT_SIZE}.csv", resource)

    # create the new owner wallet nodes
    wallet_df = multi_df.drop_duplicates(subset=["address"], keep="first")
    wallet_df.reset_index(inplace=True, drop=True)

    list_wallet_chunks = split_dataframe(wallet_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_wallet_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/wallet-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/wallets/wallet-{idx * SPLIT_SIZE}.csv", resource)

    # create signer relationships
    multi_df = multi_df.drop_duplicates(subset=["multisig", "address"], keep="first")
    multi_df.reset_index(inplace=True, drop=True)

    list_signer_chunks = split_dataframe(multi_df, SPLIT_SIZE)
    for idx, chunk in enumerate(list_signer_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/wallets/signer-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_signer_relationships(url, conn)
        set_object_private(BUCKET, f"neo/wallets/signer-{idx * SPLIT_SIZE}.csv", resource)

    # add EOA labels
    eoa_df = pd.DataFrame(eoa_list)
    eoa_df.drop_duplicates(subset=["address"], inplace=True)
    eoa_df.reset_index(drop=True, inplace=True)
    print(f"{len(eoa_df)} EOA wallets")

    list_eoa_chunks = split_dataframe(eoa_df, SPLIT_SIZE)
    for idx, eoa_batch in enumerate(list_eoa_chunks):
        url = write_df_to_s3(eoa_batch, BUCKET, f"neo/wallets/eoa-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_eoa_labels(url, conn)
        set_object_private(BUCKET, f"neo/wallets/eoa-{idx * SPLIT_SIZE}.csv", resource)

    # add contract labels
    contract_df = pd.DataFrame(contract_list)
    contract_df.drop_duplicates(subset=["address"], inplace=True)
    contract_df.reset_index(drop=True, inplace=True)
    print(f"{len(contract_df)} contract wallets")

    list_contract_chunks = split_dataframe(contract_df, SPLIT_SIZE)
    for idx, contract_batch in enumerate(list_contract_chunks):
        url = write_df_to_s3(contract_batch, BUCKET, f"neo/wallets/contract-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_contract_labels(url, conn)
        set_object_private(BUCKET, f"neo/wallets/contract-{idx * SPLIT_SIZE}.csv", resource)
