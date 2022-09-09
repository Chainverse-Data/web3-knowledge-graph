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
import re

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[1]))
sys.path.append(str(Path(__file__).resolve().parents[2]))
from twitter.helpers.cypher import *
from helpers.graph import ChainverseGraph
from helpers.s3 import *
from ingestion.helpers.cypher import create_constraints


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
bad_handles = set()


def filter_batch(batch):
    rex = re.compile("^[A-Za-z0-9_]{1,15}$")
    new_batch = []
    for x in batch:
        if not bool(rex.match(x)):
            bad_handles.add(x)
        else:
            new_batch.append(x)
    return new_batch


def get_user_response(batch, retries=0):
    if retries > 10:
        return {"data": []}

    if len(batch) == 0:
        return {"data": []}

    twitter_handles_batch = ",".join(batch)
    x = requests.get(
        f"https://api.twitter.com/2/users/by?usernames={twitter_handles_batch}&user.fields=description,id,name,public_metrics,verified,profile_image_url",
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


if __name__ == "__main__":

    create_constraints(conn)

    twitter_handles = get_recent_twitter(cutoff, conn)
    print(f"Found {len(twitter_handles)} twitter handles")

    user_list = []
    for idx in range(0, len(twitter_handles), BATCH_SIZE):
        twitter_handles_batch = twitter_handles[idx : idx + BATCH_SIZE]
        batch = filter_batch(twitter_handles_batch)
        set_items = set(twitter_handles_batch)
        resp = get_user_response(batch)
        print(f"Got {len(resp['data'])} users")
        for user in resp["data"]:
            current_dict = {}
            current_dict["name"] = user["name"]
            current_dict["handle"] = user["username"].lower()
            current_dict["bio"] = user["description"]
            current_dict["verified"] = user["verified"]
            current_dict["userId"] = user["id"]
            current_dict["followerCount"] = user["public_metrics"]["followers_count"]
            current_dict['profileImageUrl'] = user['profile_image_url']
            set_items.remove(current_dict["handle"])
            user_list.append(current_dict)

        bad_handles.update(set_items)

    user_df = pd.DataFrame(user_list)
    print(f"Found total {len(user_df)} users")

    user_df_chunks = split_dataframe(user_df, SPLIT_SIZE)
    for idx, chunk in enumerate(user_df_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/twitter/account-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_twitter_node_info(url, conn)
        set_object_private(BUCKET, f"neo/twitter/account-{idx * SPLIT_SIZE}.csv", resource)

    # add trash labels to bad handle nodes
    bad_handles_dict = [{"handle": x} for x in bad_handles]
    bad_handles_df = pd.DataFrame(bad_handles_dict)
    print(f"Found {len(bad_handles_df)} bad handles")

    bad_handles_df_chunks = split_dataframe(bad_handles_df, SPLIT_SIZE)
    for idx, chunk in enumerate(bad_handles_df_chunks):
        url = write_df_to_s3(chunk, BUCKET, f"neo/twitter/bad-{idx * SPLIT_SIZE}.csv", resource, s3)
        add_trash_labels(url, conn)
        set_object_private(BUCKET, f"neo/twitter/bad-{idx * SPLIT_SIZE}.csv", resource)

    conn.close()
