from Crypto.Hash import keccak
from rlp.utils import decode_hex, encode_hex
import rlp
import pandas as pd
import sys
from pathlib import Path
import boto3
from dotenv import load_dotenv
import os
from web3 import Web3
from tqdm import tqdm
from joblib import Parallel, delayed

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.helpers.util import tqdm_joblib
from ingestion.helpers.graph import ChainverseGraph

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

SPLIT_SIZE = 20000

provider = "https://eth-mainnet.alchemyapi.io/v2/" + str(os.environ.get("ALCHEMY_API_KEY"))
w3 = Web3(Web3.HTTPProvider(provider))

sha3_256 = lambda x: keccak.new(digest_bits=256, data=x).digest()


def to_string(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return bytes(value, "utf-8")
    if isinstance(value, int):
        return bytes(str(value), "utf-8")


def sha3(seed):
    return sha3_256(to_string(seed))


def normalize_address(x, allow_blank=False):
    if allow_blank and x == "":
        return ""
    if len(x) in (42, 50) and x[:2] == "0x":
        x = x[2:]
    if len(x) in (40, 48):
        x = decode_hex(x)
    if len(x) == 24:
        assert len(x) == 24 and sha3(x[:20])[:4] == x[-4:]
        x = x[:20]
    if len(x) != 20:
        raise Exception("Invalid address format: %r" % x)
    return x


def mk_contract_address(sender, nonce):
    return "0x" + encode_hex(sha3(rlp.encode([normalize_address(sender), nonce]))[12:])


def get_contracts(address_dict, provider):
    address = address_dict["w.address"]
    start = address_dict["start"]
    end = address_dict["end"]
    address = Web3.toChecksumAddress(address)
    w3 = Web3(Web3.HTTPProvider(provider))

    contract_list = []
    for nonce in range(start, end):
        contract = mk_contract_address(address, nonce)
        contract = Web3.toChecksumAddress(contract)
        if w3.eth.getCode(contract).hex() == "0x":
            continue
        contract_list.append({"address": address, "contract": contract})

    return contract_list


if __name__ == "__main__":

    df = pd.read_csv("ingestion/tokens/addresses.csv")
    print(f"Loaded {len(df)} addresses")

    if "end" not in df.columns:
        tqdm.pandas()
        df["end"] = df["w.address"].progress_apply(lambda x: w3.eth.getTransactionCount(Web3.toChecksumAddress(x)))
    df.to_csv("ingestion/tokens/addresses.csv", index=False)

    if "start" not in df.columns:
        df["start"] = df["end"].apply(lambda x: int((0.95 * x)))

    print(sum(df["end"]) - sum(df["start"]))
    df_dict = df.to_dict("records")

    with tqdm_joblib(tqdm(desc="Getting Contracts", total=len(df_dict))) as progress_bar:
        all_contracts = Parallel(n_jobs=-1)(delayed(get_contracts)(address_dict, provider) for address_dict in df_dict)

    all_contracts = [contract for sublist in all_contracts for contract in sublist]
    contract_df = pd.DataFrame(all_contracts)
    print(f"Found {len(contract_df)} contracts")

    df.drop(columns=["start"], inplace=True)
    df = df.rename(columns={"end": "start"})
    df.to_csv("ingestion/tokens/addresses.csv", index=False)
    contract_df.to_csv("ingestion/wallets/contracts.csv", index=False)
