import boto3
from datetime import datetime
import json
from neo4j import GraphDatabase
import pandas as pd
import s3fs
from dotenv import load_dotenv
import sys

sys.path.append(".")
import os

from daohaus.helpers.cypher import create_dao_nodes, create_member_nodes, create_proposal_nodes, create_token_nodes
from daohaus.helpers.proposals import label_desc, label_proposal_type, label_status, label_title
from helpers.s3 import *


class ChainverseGraph:
    def __init__(self, uri, user, password):
        self.__driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response


if __name__ == "__main__":

    load_dotenv()
    uri = os.getenv("NEO_URI")
    username = os.getenv("NEO_USERNAME")
    password = os.getenv("NEO_PASSWORD")
    conn = ChainverseGraph(uri, username, password)

    resource = boto3.resource("s3")
    s3 = boto3.client("s3")
    BUCKET = "chainverse"

    member_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/member.csv")
    token_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/token.csv")
    proposal_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/proposal.csv")
    dao_df = read_df_from_s3(BUCKET, "neo/daohaus/raw/dao.csv")

    # cleaning token dataframe
    temp_df = token_df.copy(deep=True)
    temp_df["address"] = temp_df["tokenAddress"]
    temp_df = temp_df.drop(columns=["whitelisted", "tokenAddress", "id"])

    # write token nodes to graph and then set private
    url = write_df_to_s3(temp_df, BUCKET, "neo/daohaus/nodes/token.csv", resource, s3)
    print("Token nodes: ", len(temp_df))
    create_token_nodes(url, conn)
    set_object_private(BUCKET, "neo/daohaus/nodes/token.csv", resource)

    # cleaning member dataframe
    temp_df = member_df.copy(deep=True)
    temp_df["kicked"].fillna(False, inplace=True)
    temp_df["jailed"].fillna(False, inplace=True)
    temp_df["shares"].fillna(-1, inplace=True)
    temp_df["loot"].fillna(-1, inplace=True)
    temp_df.rename(columns={"createdAt": "joinDate"}, inplace=True)
    temp_df["address"] = temp_df.id.apply(lambda x: x.split("-")[2])
    temp_df["daoAddress"] = temp_df.id.apply(lambda x: x.split("-")[0])

    # write member nodes to graph and then set private
    url = write_df_to_s3(temp_df, BUCKET, "neo/daohaus/nodes/member.csv", resource, s3)
    print("Member nodes: ", len(temp_df))
    create_member_nodes(url, conn)
    set_object_private(BUCKET, "neo/daohaus/nodes/member.csv", resource)

    # cleaning dao nodes
    pd.options.mode.chained_assignment = None
    temp_df = dao_df.copy(deep=True)
    temp_df = temp_df[
        [
            "id",
            "version",
            "deleted",
            "createdAt",
            "totalShares",
            "totalLoot",
            "periodDuration",
            "votingPeriodLength",
            "gracePeriodLength",
            "proposalDeposit",
            "dilutionBound",
            "processingReward",
            "version",
            "chain",
            "chain_id",
        ]
    ]
    temp_df.rename(columns={"id": "address", "version": "molochVersion"}, inplace=True)
    temp_df = temp_df[temp_df["address"].notna()]
    temp_df["deleted"].fillna(False, inplace=True)
    temp_df["molochVersion"].fillna(-1, inplace=True)
    temp_df["totalShares"].fillna(-1, inplace=True)
    temp_df["totalLoot"].fillna(-1, inplace=True)
    temp_df["periodDuration"].fillna(-1, inplace=True)
    temp_df["votingPeriodLength"].fillna(-1, inplace=True)
    temp_df["gracePeriodLength"].fillna(-1, inplace=True)
    temp_df["proposalDeposit"] = temp_df["proposalDeposit"].apply(lambda x: str(x))
    temp_df["processingReward"] = temp_df["processingReward"].apply(lambda x: str(x))

    # write dao nodes to graph and then set private
    url = write_df_to_s3(temp_df, BUCKET, "neo/daohaus/nodes/dao.csv", resource, s3)
    print("DAO nodes: ", len(temp_df))
    create_dao_nodes(url, conn)
    set_object_private(BUCKET, "neo/daohaus/nodes/dao.csv", resource)

    # add neceesary fields to proposal dataframe
    proposal_df["proposalType"] = proposal_df.apply(lambda row: label_proposal_type(row), axis=1)
    proposal_df["status"] = proposal_df.apply(lambda row: label_status(row), axis=1)
    proposal_df["title"] = proposal_df.apply(lambda row: label_title(row), axis=1)
    proposal_df["description"] = proposal_df.apply(lambda row: label_desc(row), axis=1)

    # cleaning proposal nodes
    temp_df = proposal_df.copy(deep=True)
    temp_df = temp_df[
        [
            "id",
            "createdAt",
            "title",
            "description",
            "votingPeriodStarts",
            "votingPeriodEnds",
            "gracePeriodEnds",
            "yesVotes",
            "noVotes",
            "yesShares",
            "noShares",
            "proposalType",
            "status",
            "sharesRequested",
            "lootRequested",
            "tributeOffered",
            "molochVersion",
            "chain",
            "chain_id",
        ]
    ]
    temp_df["votingPeriodStarts"].fillna(-1, inplace=True)
    temp_df["votingPeriodEnds"].fillna(-1, inplace=True)
    temp_df["gracePeriodEnds"].fillna(-1, inplace=True)
    temp_df["yesVotes"].fillna(-1, inplace=True)
    temp_df["noVotes"].fillna(-1, inplace=True)
    temp_df["yesShares"].fillna(-1, inplace=True)
    temp_df["noShares"].fillna(-1, inplace=True)
    temp_df["proposalType"].fillna("", inplace=True)
    temp_df["status"].fillna("", inplace=True)
    temp_df["sharesRequested"].fillna(-1, inplace=True)
    temp_df["lootRequested"].fillna(-1, inplace=True)
    temp_df["tributeOffered"].fillna(-1, inplace=True)
    temp_df["molochVersion"].fillna(-1, inplace=True)

    # write proposal nodes to graph and then set private
    url = write_df_to_s3(temp_df, BUCKET, "neo/daohaus/nodes/proposal.csv", resource, s3)
    print("Proposal nodes: ", len(temp_df))
    create_proposal_nodes(url, conn)
    set_object_private(BUCKET, "neo/daohaus/nodes/proposal.csv", resource)

    # close neo connection
    conn.close()
