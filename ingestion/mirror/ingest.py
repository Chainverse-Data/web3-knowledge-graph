from distutils.filelist import findall
import boto3
from datetime import datetime
import json
import pandas as pd
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
import re

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[1]))
from mirror.helpers.cypher_nodes import *
from mirror.helpers.cypher_relationships import *
from helpers.s3 import *
from helpers.graph import ChainverseGraph


def getTwitterAccounts(x):
    twitter_account_list = re.findall("twitter.com\/[\w]+", x)
    return [x.split("/")[-1] for x in twitter_account_list]


SPLIT_SIZE = 10000
now = datetime.now()

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

if __name__ == "__main__":

    # create space nodes
    content_object = s3.get_object(Bucket="chainverse", Key=f"mirror/final/{now.strftime('%m-%d-%Y')}/data.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    entry_list = []
    twitter_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["title"] = entry["title"] or ""
        current_dict["publication"] = entry["publication"] or ""
        current_dict["contributor"] = entry["contributer"].lower() or ""
        current_dict["datePublished"] = entry.get("timestamp", 0)
        current_dict["arweaveTx"] = entry["transaction"] or ""
        current_dict["body"] = entry["body"] or ""

        twitter_accounts = getTwitterAccounts(current_dict["body"])
        for account in twitter_accounts:
            twitter_dict = {}
            twitter_dict["twitter"] = account
            twitter_dict["arweaveTx"] = current_dict["arweaveTx"]
            twitter_list.append(twitter_dict)

        entry_list.append(current_dict)

    entry_df = pd.DataFrame(entry_list)
    entry_df.drop_duplicates(subset=["arweaveTx"], inplace=True)
    print(f"{len(entry_df)} entries")

    all_wallets = list(set(entry_df["contributor"]))
    wallet_dict = [{"address": wallet} for wallet in all_wallets if wallet != ""]
    wallet_df = pd.DataFrame(wallet_dict)
    print(f"{len(wallet_df)} wallets")

    # create all wallet nodes
    list_wallet_chunks = split_dataframe(wallet_df, SPLIT_SIZE)
    for idx, wallet_batch in enumerate(list_wallet_chunks):
        url = write_df_to_s3(
            wallet_batch, BUCKET, f"neo/mirror/nodes/wallet/wallet-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/mirror/nodes/wallet/wallet-{idx * SPLIT_SIZE}.csv", resource)

    # create all twitter nodes
    list_twitter_chunks = split_dataframe(pd.DataFrame(twitter_list), SPLIT_SIZE)
    for idx, twitter_batch in enumerate(list_twitter_chunks):
        url = write_df_to_s3(
            twitter_batch, BUCKET, f"neo/mirror/nodes/twitter/twitter-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_twitter_nodes(url, conn)
        set_object_private(BUCKET, f"neo/mirror/nodes/twitter/twitter-{idx * SPLIT_SIZE}.csv", resource)

    # create article nodes
    list_article_chunks = split_dataframe(entry_df, SPLIT_SIZE)
    for idx, article_batch in enumerate(list_article_chunks):
        url = write_df_to_s3(
            article_batch, BUCKET, f"neo/mirror/nodes/article/article-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_article_nodes(url, conn)
        set_object_private(BUCKET, f"neo/mirror/nodes/article/article-{idx * SPLIT_SIZE}.csv", resource)

    # create author relationships
    rel_df = entry_df[["arweaveTx", "contributor"]]
    list_rel_chunks = split_dataframe(rel_df, SPLIT_SIZE)
    for idx, rel_batch in enumerate(list_rel_chunks):
        url = write_df_to_s3(
            rel_batch, BUCKET, f"neo/mirror/relationships/author/author-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_author_relationships(url, conn)
        set_object_private(BUCKET, f"neo/mirror/relationships/author/author-{idx * SPLIT_SIZE}.csv", resource)

