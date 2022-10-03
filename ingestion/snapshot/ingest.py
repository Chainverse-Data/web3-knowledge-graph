import boto3
from datetime import datetime, timedelta
import json
import pandas as pd
from dotenv import load_dotenv
import os
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.snapshot.helpers.cypher_nodes import *
from ingestion.snapshot.helpers.cypher_relationships import *
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.cypher import (
    create_constraints,
    create_indexes,
    merge_ens_nodes,
    merge_ens_relationships,
    merge_wallet_nodes,
    merge_twitter_nodes,
    merge_token_nodes,
)

SPLIT_SIZE = 20000
now = datetime.now() - timedelta(days=10)  # adjust days as needed

load_dotenv()
uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

if __name__ == "__main__":

    # create constraints and indexes
    create_constraints(conn)
    create_indexes(conn)

    # create space nodes
    content_object = s3.get_object(Bucket="chainverse", Key=f"snapshot/spaces/{now.strftime('%m-%d-%Y')}/spaces.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    # clean space nodes
    space_list = []
    strategy_list = []
    ens_list = []
    member_list = []
    admin_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["snapshotId"] = entry["id"]
        current_dict[
            "profileUrl"
        ] = f"https://cdn.stamp.fyi/space/{current_dict['snapshotId']}?s=160&cb=a1b40604488c19a1"
        current_dict["name"] = entry["name"]
        current_dict["about"] = entry.get("about", "").replace('"', "").replace("'", "").replace("\\", "").strip()
        current_dict["chainId"] = entry.get("network", "")
        current_dict["symbol"] = entry.get("symbol", "")
        current_dict["handle"] = entry.get("twitter", "")
        current_dict["handle"] = current_dict["handle"] if current_dict["handle"] else ""
        current_dict["twitterProfileUrl"] = "https://twitter.com/" + current_dict["handle"]

        try:
            current_dict["minScore"] = entry["filters"]["minScore"]
        except:
            current_dict["minScore"] = -1

        try:
            current_dict["onlyMembers"] = entry["filters"]["onlyMembers"]
        except:
            current_dict["onlyMembers"] = False

        if "strategies" in entry and entry["strategies"]:
            for strategy in entry["strategies"]:
                strategy_list.append({"space": entry["id"], "strategy": strategy})

        for member in entry["members"]:
            member_list.append({"space": entry["id"], "address": member.lower()})

        for admin in entry["admins"]:
            admin_list.append({"space": entry["id"], "address": admin.lower()})

        if "ens" in entry:
            ens = entry["ens"]
            ens_list.append(
                {
                    "name": entry["id"],
                    "owner": ens["owner"].lower(),
                    "tokenId": ens["token_id"],
                    "txHash": ens["trans"]["hash"],
                    "contractAddress": ens["trans"]["rawContract"]["address"],
                }
            )

        space_list.append(current_dict)

    space_df = pd.DataFrame(space_list)
    space_df.drop_duplicates(subset=["snapshotId"], inplace=True)
    print("Space nodes: ", len(space_df))

    list_space_chunks = split_dataframe(space_df, SPLIT_SIZE)
    for idx, space_batch in enumerate(list_space_chunks):
        url = write_df_to_s3(
            space_batch, BUCKET, f"neo/snapshot/nodes/space/space-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_space_nodes(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/space/space-{idx * SPLIT_SIZE}.csv", resource)

    # create ens nodes and relationships
    ens_df = pd.DataFrame(ens_list)
    ens_df.drop_duplicates(subset=["name"], inplace=True)
    print("ENS nodes: ", len(ens_df))

    list_ens_chunks = split_dataframe(ens_df, SPLIT_SIZE)
    for idx, ens_batch in enumerate(list_ens_chunks):
        url = write_df_to_s3(ens_batch, BUCKET, f"neo/snapshot/nodes/ens/ens-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_ens_nodes(url, conn)
        merge_ens_relationships(url, conn)
        merge_space_alias_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/ens/ens-{idx * SPLIT_SIZE}.csv", resource)

    # create member nodes
    member_df = pd.DataFrame(member_list)
    member_df.drop_duplicates(subset=["space", "address"], inplace=True)
    print("Member nodes: ", len(member_df))
    for idx, member_batch in enumerate(split_dataframe(member_df, SPLIT_SIZE)):
        url = write_df_to_s3(
            member_batch, BUCKET, f"neo/snapshot/nodes/member/member-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_wallet_nodes(url, conn)
        merge_member_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/member/member-{idx * SPLIT_SIZE}.csv", resource)

    # create admin nodes
    admin_df = pd.DataFrame(admin_list)
    admin_df.drop_duplicates(subset=["space", "address"], inplace=True)
    print("Admin nodes: ", len(admin_df))
    for idx, admin_batch in enumerate(split_dataframe(admin_df, SPLIT_SIZE)):
        url = write_df_to_s3(
            admin_batch, BUCKET, f"neo/snapshot/nodes/admin/admin-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_wallet_nodes(url, conn)
        merge_admin_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/admin/admin-{idx * SPLIT_SIZE}.csv", resource)

    # create twitter nodes
    twitter_df = space_df[["snapshotId", "handle", "twitterProfileUrl"]].drop_duplicates(subset=["handle"])
    twitter_df = twitter_df[twitter_df["handle"] != ""]
    print("Twitter nodes: ", len(twitter_df))

    list_twitter_chunks = split_dataframe(twitter_df, SPLIT_SIZE)
    for idx, twitter_batch in enumerate(list_twitter_chunks):
        url = write_df_to_s3(
            twitter_batch, BUCKET, f"neo/snapshot/nodes/twitter/twitter-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_twitter_nodes(url, conn)
        merge_twitter_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/twitter/twitter-{idx * SPLIT_SIZE}.csv", resource)

    # create token nodes and strategy relationships
    strategy_relationships = []
    token_list = []
    for item in strategy_list:
        current_dict = {}
        space = item.get("space", "")
        if space == "":
            continue
        current_dict["space"] = space

        entry = item.get("strategy", "")
        if entry == "":
            continue

        try:
            token_dict = {}
            params = entry.get("params", "")
            if params == "":
                continue
            address = params.get("address", "")
            if address == "" or not isinstance(address, str):
                continue
            token_dict["address"] = address.lower()
            token_dict["symbol"] = params.get("symbol", "")
            token_dict["decimals"] = params.get("decimals", -1)
            current_dict["token"] = token_dict["address"]
            token_list.append(token_dict)
            strategy_relationships.append(current_dict)
        except:
            continue

    token_df = pd.DataFrame(token_list)
    token_df.drop_duplicates(subset="address", inplace=True)
    print("Token nodes: ", len(token_df))
    url = write_df_to_s3(token_df, BUCKET, "neo/snapshot/nodes/token.csv", resource, s3)
    merge_token_nodes(url, conn)
    set_object_private(BUCKET, "neo/snapshot/nodes/token.csv", resource)

    strategy_df = pd.DataFrame(strategy_relationships)
    print("Strategy Relationships: ", len(strategy_df))
    url = write_df_to_s3(strategy_df, BUCKET, "neo/snapshot/relationships/strategy.csv", resource, s3)
    merge_strategy_relationships(url, conn)
    set_object_private(BUCKET, "neo/snapshot/relationships/strategy.csv", resource)

    # create proposal nodes
    content_object = s3.get_object(
        Bucket="chainverse", Key=f"snapshot/proposals/{now.strftime('%m-%d-%Y')}/proposals.json"
    )
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    proposal_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["snapshotId"] = entry["id"]
        current_dict["ipfsCID"] = entry["ipfs"]
        current_dict["author"] = entry["author"].lower() or ""
        current_dict["createdAt"] = entry["created"] or 0
        current_dict["type"] = entry["type"] or -1
        current_dict["spaceId"] = entry["space"]["id"]

        current_dict["title"] = entry["title"].replace('"', "").replace("'", "").replace("\\", "").strip() or ""
        current_dict["text"] = entry["body"].replace('"', "").replace("'", "").replace("\\", "").strip() or ""

        choices = json.dumps(entry["choices"])
        choices = choices.replace('"', "").replace("'", "").strip() or ""

        current_dict["choices"] = choices
        current_dict["startDt"] = entry["start"] or 0
        current_dict["endDt"] = entry["end"] or 0
        current_dict["state"] = entry["state"] or ""
        current_dict["link"] = entry["link"].strip() or ""

        proposal_list.append(current_dict)

    proposal_df = pd.DataFrame(proposal_list)
    proposal_df.drop_duplicates("snapshotId", inplace=True)
    proposal_df.dropna(subset=["author"], inplace=True)
    print("Proposal Nodes: ", len(proposal_df))

    list_prop_chunks = split_dataframe(proposal_df, SPLIT_SIZE)
    for idx, prop_batch in enumerate(list_prop_chunks):
        url = write_df_to_s3(
            prop_batch, BUCKET, f"neo/snapshot/nodes/proposal/prop-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_proposal_nodes(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/proposal/prop-{idx * SPLIT_SIZE}.csv", resource)

    # create proposal - space relationships
    proposal_space_df = proposal_df[["snapshotId", "spaceId"]]
    list_prop_chunks = split_dataframe(proposal_space_df, SPLIT_SIZE)
    for idx, prop_batch in enumerate(list_prop_chunks):
        url = write_df_to_s3(
            prop_batch, BUCKET, f"neo/snapshot/relationships/proposal/prop-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_proposal_space_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/relationships/proposal/prop-{idx * SPLIT_SIZE}.csv", resource)

    # get vote entries
    content_object = s3.get_object(Bucket="chainverse", Key=f"snapshot/votes/{now.strftime('%m-%d-%Y')}/votes.json")
    data = content_object["Body"].read().decode("utf-8")
    json_data = json.loads(data)

    vote_list = []
    for entry in json_data:
        current_dict = {}
        current_dict["id"] = entry["id"]
        current_dict["voter"] = entry["voter"].lower() or ""
        if current_dict["voter"] == "":
            print("x")
        current_dict["votedAt"] = entry["created"]
        current_dict["ipfs"] = entry["ipfs"]

        try:
            current_dict["choice"] = json.dumps(entry["choice"])
            current_dict["proposalId"] = entry["proposal"]["id"]
            current_dict["spaceId"] = entry["space"]["id"]
        except:
            print("xxx")
            continue

        vote_list.append(current_dict)

    vote_df = pd.DataFrame(vote_list)
    vote_df.drop_duplicates("id", inplace=True)
    print(f"Vote entries: {len(vote_df)}")

    all_wallets = set(list(vote_df["voter"]) + list(proposal_df["author"]))
    wallet_dict = [{"address": wallet} for wallet in all_wallets if wallet != ""]
    wallet_df = pd.DataFrame(wallet_dict)
    wallet_df.drop_duplicates("address", inplace=True)
    wallet_df.dropna(subset=["address"], inplace=True)
    print(f"Wallet entries: {len(wallet_df)}")

    # create all wallet nodes
    list_wallet_chunks = split_dataframe(wallet_df, SPLIT_SIZE)
    for idx, wallet_batch in enumerate(list_wallet_chunks):
        url = write_df_to_s3(
            wallet_batch, BUCKET, f"neo/snapshot/nodes/wallet/wallet-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_wallet_nodes(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/wallet/wallet-{idx * SPLIT_SIZE}.csv", resource)

    # create proposal - author relationships
    list_prop_chunks = split_dataframe(proposal_df, SPLIT_SIZE)
    for idx, prop_batch in enumerate(list_prop_chunks):
        url = write_df_to_s3(
            prop_batch, BUCKET, f"neo/snapshot/nodes/proposal/prop-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_proposal_author_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/nodes/proposal/prop-{idx * SPLIT_SIZE}.csv", resource)

    # create vote relationships
    list_vote_chunks = split_dataframe(vote_df, SPLIT_SIZE)
    for idx, vote_batch in enumerate(list_vote_chunks):
        url = write_df_to_s3(
            vote_batch, BUCKET, f"neo/snapshot/relationships/votes/vote-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_vote_relationships(url, conn)
        set_object_private(BUCKET, f"neo/snapshot/relationships/votes/vote-{idx * SPLIT_SIZE}.csv", resource)
