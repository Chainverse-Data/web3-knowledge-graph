from heapq import merge
import requests
from dotenv import load_dotenv
import os
import sys
import json
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import boto3
import time
from joblib import Parallel, delayed
from web3 import Web3
import eth_utils
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.s3 import *
from ingestion.helpers.cypher import merge_twitter_nodes, merge_ens_nodes, merge_ens_relationships
from ingestion.helpers.util import tqdm_joblib
from ingestion.twitter.helpers.cypher import add_trash_labels, add_twitter_node_info, merge_twitter_ens_relationships
from scraping.snapshot.helpers.util import get_ens


def get_user_response(batch, retries=0):
    if retries > 10:
        return {"data": []}

    if len(batch) == 0:
        return {"data": []}

    joined_batch = ",".join(batch)
    x = requests.get(
        f"https://api.twitter.com/2/users?ids={joined_batch}&user.fields=username,description,id,name,public_metrics,verified,profile_image_url",
        headers=headers,
    )
    resp = json.loads(x.text)
    head = dict(x.headers)
    if "data" not in resp and resp["title"] == "Too Many Requests":
        end_time = head["x-rate-limit-reset"]
        epoch_time = int(time.time())
        time_to_wait = int(end_time) - epoch_time
        print(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
        time.sleep(time_to_wait)
        return get_user_response(batch, retries=retries + 1)

    return resp


load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

now = datetime.now()
cutoff = now - timedelta(days=1)

BATCH_SIZE = 100
SPLIT_SIZE = 10000

headers = {
    "Authorization": f"Bearer {os.environ.get('TWITTER_BEARER_TOKEN')}",
}

provider = "https://eth-mainnet.alchemyapi.io/v2/" + str(os.environ.get("ALCHEMY_API_KEY"))
w3 = Web3(Web3.HTTPProvider(provider))


if __name__ == "__main__":

    with open("ingestion/ultrasoundmoney/ens_twitter.json", "r") as f:
        ens_twitter = json.load(f)

    print(len(ens_twitter))

    id_ens_dict = {}

    for idx, entry in enumerate(ens_twitter):
        ens_twitter[idx]["name"] = entry["name"].split(".")[0] + ".eth"
        id_ens_dict[entry["id"]] = entry["name"]

    try:
        with open("ingestion/ultrasoundmoney/twitter_users.json", "r") as f:
            all_resp = json.load(f)

    except:

        all_resp = []
        for idx in range(0, len(ens_twitter), BATCH_SIZE):
            batch = ens_twitter[idx : idx + BATCH_SIZE]
            batch = [x["id"] for x in batch]
            resp = get_user_response(batch)
            all_resp.extend(resp["data"])

        with open("ingestion/ultrasoundmoney/twitter_users.json", "w") as f:
            json.dump(all_resp, f)

    user_list = []
    for user in all_resp:
        current_dict = {}
        current_dict["name"] = user["name"]
        current_dict["handle"] = user["username"].lower()
        current_dict["bio"] = user["description"]
        current_dict["verified"] = user["verified"]
        current_dict["userId"] = user["id"]
        current_dict["followerCount"] = user["public_metrics"]["followers_count"]
        current_dict["profileImageUrl"] = user["profile_image_url"]
        user_list.append(current_dict)

    user_df = pd.DataFrame(user_list)
    print(f"Found total {len(user_df)} users")

    user_df_chunks = split_dataframe(user_df, SPLIT_SIZE)
    for idx, chunk in enumerate(user_df_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/ultrasound/account-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_twitter_nodes(url, conn)
        add_twitter_node_info(url, conn)
        set_object_private(BUCKET, f"neo/ultrasound/account-{idx * SPLIT_SIZE}.csv", resource)

    ens_name_list = []
    for entry in user_list:
        ens_name_list.append({"id": id_ens_dict[entry["userId"]]})

    with tqdm_joblib(tqdm(desc="Getting ENS Data", total=len(ens_name_list))) as progress_bar:
        # ens_list = Parallel(n_jobs=-1)(delayed(get_ens)(name, provider) for name in ens_name_list)
        ens_list = []

    ens_list = [x for x in ens_list if x is not None]
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
    print("ENS nodes: ", len(ens_df))

    list_ens_chunks = split_dataframe(ens_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_ens_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/ultrasound/nodes/ens/ens-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_ens_nodes(url, conn)
        merge_ens_relationships(url, conn)
        set_object_private(BUCKET, f"neo/ultrasound/nodes/ens/ens-{idx * SPLIT_SIZE}.csv", resource)

    twitter_ens_list = []
    for entry in user_list:
        twitter_id = entry["userId"]
        ens_name = id_ens_dict[twitter_id]
        current_dict = {"handle": entry["handle"], "ens": ens_name}
        twitter_ens_list.append(current_dict)

    twitter_ens_df = pd.DataFrame(twitter_ens_list)
    twitter_ens_df.drop_duplicates(subset=["handle"], inplace=True)
    print("Twitter ENS relationships: ", len(twitter_ens_df))

    list_twitter_ens_chunks = split_dataframe(twitter_ens_df, SPLIT_SIZE)
    for idx, twitter_ens_batch in enumerate(list_twitter_ens_chunks):
        url = write_df_to_s3(
            twitter_ens_batch,
            BUCKET,
            f"neo/ultrasound/relationships/twitter-ens/twitter-ens-{idx * SPLIT_SIZE}.csv",
            resource,
            s3,
        )
        merge_twitter_ens_relationships(url, conn)
        set_object_private(
            BUCKET, f"neo/ultrasound/relationships/twitter-ens/twitter-ens-{idx * SPLIT_SIZE}.csv", resource
        )

    conn.close()
