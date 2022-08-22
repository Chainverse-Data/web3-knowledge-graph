import boto3
from datetime import datetime
import json
from neo4j import GraphDatabase
import pandas as pd
import s3fs
from dotenv import load_dotenv
import sys
import os
sys.path.append(".")

from daohaus.helpers.cypher import (
    create_dao_relationships,
    create_approved_token_relationships,
    create_member_relationships,
    create_proposal_relationships,
    create_vote_relationships,
)
from helpers.s3 import write_df_to_s3, set_object_private, read_df_from_s3
from helpers.graph import ChainverseGraph


if __name__ == "__main__":

    load_dotenv()
    uri = os.getenv("NEO_URI")
    username = os.getenv("NEO_USERNAME")
    password = os.getenv("NEO_PASSWORD")
    conn = ChainverseGraph(uri, username, password)

    resource = boto3.resource("s3")
    s3 = boto3.client("s3")
    BUCKET = "chainverse"

    # read in raw csvs from s3
    member_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/member.csv")
    token_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/token.csv")
    proposal_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/proposal.csv")
    dao_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/dao.csv")

    dao_json = dao_df.to_dict("records")
    primary_dao_list = []
    approved_token_list = []
    for entry in dao_json:
        current_dict = {}
        current_dict["dao"] = entry["id"]

        try:
            current_dict["summoner"] = str(entry["summoner"])
        except:
            current_dict["summoner"] = ""

        try:
            current_dict["depositToken"] = entry["depositToken"]["id"].split("-")[2]
        except:
            current_dict["depositToken"] = ""

        try:
            approvedTokens = entry["approvedTokens"].replace("'", '"')
            approvedTokens = json.loads(approvedTokens)
        except:
            approvedTokens = []

        for token in approvedTokens:
            try:
                approved_token_list.append({"dao": current_dict["dao"], "approvedToken": token["id"].split("-")[2]})
            except:
                print("failed approved token")
                continue

        primary_dao_list.append(current_dict)

    # create dao primary relationships
    primary_dao_relationship_df = pd.DataFrame(primary_dao_list)
    url = write_df_to_s3(primary_dao_relationship_df, BUCKET, "neo/daohaus/relationships/dao.csv", resource, s3)
    #create_dao_relationships(url, conn)
    set_object_private(BUCKET, "neo/daohaus/relationships/dao.csv", resource)

    # create approved token relationships
    approved_token_relationship_df = pd.DataFrame(approved_token_list)
    url = write_df_to_s3(
        approved_token_relationship_df, BUCKET, "neo/daohaus/relationships/approved_token.csv", resource, s3
    )
    #create_approved_token_relationships(url, conn)
    set_object_private(BUCKET, "neo/daohaus/relationships/approved_token.csv", resource)

    member_json = member_df.to_dict("records")
    member_dao_list = []
    for entry in member_json:
        dao_id = entry["id"].split("-")[0]
        member_address = entry["id"].split("-")[2]
        member_dao_list.append({"dao": dao_id, "member": member_address})

    # create primary member relationships
    member_dao_df = pd.DataFrame(member_dao_list)
    url = write_df_to_s3(member_dao_df, BUCKET, "neo/daohaus/relationships/member.csv", resource, s3)
    #create_member_relationships(url, conn)
    set_object_private(BUCKET, "neo/daohaus/relationships/member.csv", resource)

    proposal_json = proposal_df.to_dict("records")
    proposal_dao_list = []
    vote_list = []
    for entry in proposal_json:
        current_dict = {}
        current_dict["dao"] = entry["molochAddress"]
        current_dict["applicant"] = entry["applicant"]
        current_dict["sponsor"] = entry["sponsor"]
        current_dict["proposer"] = entry["proposer"]
        current_dict["processor"] = entry["processor"]
        current_dict["proposal"] = entry["id"]

        try:
            fixedVotes = entry["votes"].replace("'", '"')
            fixedVotes = json.loads(fixedVotes)
        except:
            fixedVotes = []

        for vote in fixedVotes:
            try:
                vote_dict = {}
                vote_dict["dao"] = current_dict["dao"]
                vote_dict["proposal"] = current_dict["proposal"]
                vote_dict["createdAt"] = vote["createdAt"]
                vote_dict["vote"] = vote["uintVote"]
                vote_dict["member"] = vote["id"].split("-")[2]
                vote_list.append(vote_dict)

            except:
                print("failed vote")
                continue

        proposal_dao_list.append(current_dict)

    proposal_dao_df = pd.DataFrame(proposal_dao_list)
    url = write_df_to_s3(proposal_dao_df, BUCKET, "neo/daohaus/relationships/proposal.csv", resource, s3)
    create_proposal_relationships(url, conn)
    set_object_private(BUCKET, "neo/daohaus/relationships/proposal.csv", resource)

    vote_rel_df = pd.DataFrame(vote_list)
    url = write_df_to_s3(vote_rel_df, BUCKET, "neo/daohaus/relationships/votes.csv", resource, s3)
    create_vote_relationships(url, conn)
    set_object_private(BUCKET, "neo/daohaus/relationships/votes.csv", resource)

    conn.close()

