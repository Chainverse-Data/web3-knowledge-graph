from distutils.filelist import findall
import boto3
from datetime import datetime, timedelta
import json
import pandas as pd
from dotenv import load_dotenv
import os
import sys
from pathlib import Path
import re

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.mirror.helpers.cypher_nodes import *
from ingestion.mirror.helpers.cypher_relationships import *
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.cypher import merge_wallet_nodes, create_constraints, create_indexes, merge_twitter_nodes

SPLIT_SIZE = 10000
now = datetime.now() - timedelta(days=10)

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"


def getTwitterAccounts(x):
    twitter_account_list = re.findall("twitter.com\/[\w]+", x)
    return [x.split("/")[-1] for x in twitter_account_list]


if __name__ == "__main__":

    create_constraints(conn)
    create_indexes(conn)

    # create space nodes
    content_object = s3.get_object(Bucket="chainverse", Key=f"mirror/final/{now.strftime('%m-%d-%Y')}/data.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    entry_list = []
    twitter_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["title"] = entry["title"] or ""
        current_dict["title"] = current_dict["title"].replace('"', "'")
        current_dict["publication"] = entry["publication"] or ""
        current_dict["publication"] = current_dict["publication"].replace('"', "'")
        current_dict["contributor"] = entry["contributor"].lower() or ""
        current_dict["contributor"] = current_dict["contributor"].replace('"', "'")
        current_dict["datePublished"] = entry.get("timestamp", 0)
        current_dict["arweaveTx"] = entry["transaction"] or ""
        current_dict["arweaveTx"] = current_dict["arweaveTx"].replace('"', "'")
        current_dict["text"] = entry["body"] or ""
        while '"' in current_dict["text"]:
            current_dict["text"] = current_dict["text"].replace('"', "'")

        twitter_accounts = getTwitterAccounts(current_dict["text"])
        for account in twitter_accounts:
            twitter_dict = {}
            twitter_dict["handle"] = account
            twitter_dict["arweaveTx"] = current_dict["arweaveTx"]
            twitter_dict["twitterProfileUrl"] = "https://twitter.com/" + twitter_dict["handle"]
            twitter_list.append(twitter_dict)

        entry_list.append(current_dict)

    entry_df = pd.DataFrame(entry_list)
    entry_df.drop_duplicates(subset=["arweaveTx"], inplace=True)
    entry_df = entry_df[entry_df["arweaveTx"] != ""]
    print(f"{len(entry_df)} entries")

    all_wallets = list(set(entry_df["contributor"]))
    wallet_dict = [{"address": wallet} for wallet in all_wallets if wallet != ""]
    wallet_df = pd.DataFrame(wallet_dict)
    print(f"{len(wallet_df)} wallets")

    twitter_df = pd.DataFrame(twitter_list)
    twitter_df.drop_duplicates(subset=["handle"], inplace=True)
    print(f"{len(twitter_df)} twitter handles")

    create_indexes(conn)

    # create all wallet nodes
    list_wallet_chunks = split_dataframe(wallet_df, SPLIT_SIZE)
    for idx, wallet_batch in enumerate(list_wallet_chunks):
        url = write_df_to_s3(
            wallet_batch, BUCKET, f"neo/mirror/nodes/wallet/wallet-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/mirror/nodes/wallet/wallet-{idx * SPLIT_SIZE}.csv", resource)

    # create article nodes
    article_split_size = 2000
    list_article_chunks = split_dataframe(entry_df, article_split_size)
    for idx, article_batch in enumerate(list_article_chunks):
        url = write_df_to_s3(
            article_batch, BUCKET, f"neo/mirror/nodes/article/article-{idx * article_split_size}.csv", resource, s3
        )
        merge_article_nodes(url, conn)
        set_object_private(BUCKET, f"neo/mirror/nodes/article/article-{idx * article_split_size}.csv", resource)

    # create all twitter nodes
    list_twitter_chunks = split_dataframe(twitter_df, SPLIT_SIZE)
    for idx, twitter_batch in enumerate(list_twitter_chunks):
        url = write_df_to_s3(
            twitter_batch, BUCKET, f"neo/mirror/nodes/twitter/twitter-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_twitter_nodes(url, conn)
        merge_twitter_relationships(url, conn)
        set_object_private(BUCKET, f"neo/mirror/nodes/twitter/twitter-{idx * SPLIT_SIZE}.csv", resource)

    # create author relationships
    rel_df = entry_df[["arweaveTx", "contributor"]]
    list_rel_chunks = split_dataframe(rel_df, SPLIT_SIZE)
    for idx, rel_batch in enumerate(list_rel_chunks):
        url = write_df_to_s3(
            rel_batch, BUCKET, f"neo/mirror/relationships/author/author-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_author_relationships(url, conn)
        set_object_private(BUCKET, f"neo/mirror/relationships/author/author-{idx * SPLIT_SIZE}.csv", resource)

    conn.close()

