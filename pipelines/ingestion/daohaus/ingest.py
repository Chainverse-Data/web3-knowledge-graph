from dotenv import load_dotenv
import boto3
import logging
from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport
import json
import sys
from pathlib import Path
import pandas as pd
import os
from joblib import Parallel, delayed
from tqdm import tqdm

sys.path.append(str(Path(__file__).resolve().parents[2]))
from ingestion.daohaus.helpers.cypher_nodes import *
from ingestion.daohaus.helpers.cypher_relationships import *
from ingestion.helpers.s3 import *
from ingestion.helpers.graph import ChainverseGraph
from ingestion.helpers.util import tqdm_joblib
from ingestion.helpers.cypher import (
    create_constraints,
    create_indexes,
    merge_ens_nodes,
    merge_ens_relationships,
    merge_wallet_nodes,
    merge_twitter_nodes,
    merge_token_nodes,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

uri = os.getenv("NEO_URI")
username = os.getenv("NEO_USERNAME")
password = os.getenv("NEO_PASSWORD")
conn = ChainverseGraph(uri, username, password)

client = boto3.client("s3")
resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"

SPLIT_SIZE = int(os.environ.get("SPLIT_SIZE", 20000))


def fetch_daos(subgraph_url, last_id="", ids=None):
    id_exp = "id_gt: $last_id"
    if ids:
        id_exp = "id_in: $ids"
    transport = AIOHTTPTransport(url=subgraph_url)
    client = Client(transport=transport, fetch_schema_from_transport=False)
    try:
        query = gql(
            """
            query getDaos($last_id: String, $ids: [String]) {
                moloches(first: 1000, where: { %s }) {
                id
                createdAt
                summoningTime
                deleted
                }
            }
            """
            % id_exp
        )
        resp = client.execute(query, variable_values={"last_id": last_id, "ids": ids})
        return resp.get("moloches", [])
    except Exception:
        logger.exception("Failed to fetch daos")
        return []


def get_most_recent_key(client, dao_id):
    objects = client.list_objects_v2(Bucket="chainverse", Prefix=f"daohaus/{dao_id}")
    files = objects.get("Contents")
    most_recent = {"key": "", "block": 0}
    if not files:
        return None
    for f in files:
        key = f.get("Key")
        block = key.split("/")[-1].split("_")[1]
        if most_recent.get("block", 0) < int(block):
            most_recent = {"key": key, "block": int(block)}
    return most_recent.get("key")


def fetch_daos_file(client, key):
    obj = client.get_object(Bucket="chainverse", Key=key)
    contents = obj["Body"].read()
    return json.loads(contents)


def get_chain(network):
    if network == 100:
        name = "gnosis"
    elif network == 1:
        name = "mainnet"
    return name


def clean_dao(dao, chain_id):
    profileUrl = f"https://app.daohaus.club/dao/{hex(chain_id)}/" + dao.get("id")
    q = {
        "name": dao.get("name", ""),
        "daohausId": dao.get("id"),
        "deleted": dao.get("deleted", False),
        "occurDt": dao.get("createdAt", 0),
        "chainId": chain_id,
        "chain": get_chain(chain_id),
        "totalShares": dao.get("totalShares", 0),
        "totalLoot": dao.get("totalLoot", 0),
        "version": dao.get("version"),
        "summoner": dao.get("summoner"),
        "profileUrl": profileUrl,
    }
    return q


def clean_proposal(proposal, dao_id):
    q = {
        "daohausId": proposal.get("id"),
        "author": proposal.get("applicant"),
        "sharesRequested": proposal.get("sharesRequested") or 0,
        "lootRequested": proposal.get("lootRequested") or 0,
        "tributeOffered": proposal.get("tributeOffered") or 0,
        "tributeToken": proposal.get("tributeToken"),
        "paymentRequested": proposal.get("paymentRequested") or 0,
        "paymentToken": proposal.get("paymentToken"),
        "processed": proposal.get("processed"),
        "passed": proposal.get("didPass"),
        "cancelled": proposal.get("cancelled"),
        "sponsored": proposal.get("sponsored"),
        "yesShares": proposal.get("yesShares") or 0,
        "noShares": proposal.get("noShares") or 0,
        "text": proposal.get("details").replace('"', "'").replace(",", "") if proposal.get("details") else "",
        "applicant": proposal.get("applicant"),
        "sponsor": proposal.get("sponsor"),
        "daoId": dao_id,
        "startDt": proposal.get("votingPeriodStarts"),
        "endDt": proposal.get("votingPeriodEnds"),
        "occurDt": proposal.get("createdAt"),
    }
    return q


def clean_member(member, dao_id):
    address = member.get("id").split("-")[-1]
    q = {
        "shares": member.get("shares") if member.get("shares") else 0,
        "loot": member.get("loot") if member.get("loot") else 0,
        "jailed": member.get("jailed") or False,
        "kicked": member.get("kicked") or False,
        "occurDt": member.get("createdAt", 0),
        "address": address,
        "daoId": dao_id,
    }
    return q


def clean_token(token, dao_id):
    q = {
        "address": token.get("tokenAddress"),
        "symbol": token.get("symbol") or "",
        "approved": token.get("approved") or False,
        "decimals": token.get("decimals") if token.get("decimals") else 0,
        "daoId": dao_id,
    }
    return q


def clean_vote(vote, prop_id):
    q = {
        "choice": vote.get("uintVote"),
        "votedDt": vote.get("createdAt"),
        "address": vote.get("id").split("-")[2],
        "proposalId": prop_id,
    }
    return q


def handle_dao(dao, network):
    logger.info(f"handling {dao['id']}")
    chain = get_chain(network)
    dao_dict = clean_dao(dao, network)

    member_list = []
    for member in dao.get("members", []):
        address = member.get("id").split("-")[-1]
        member_dict = clean_member(member, dao_dict["daohausId"])
        member_list.append(member_dict)

    token_list = []
    for token in dao.get("tokens", []):
        token_dict = clean_token(token, dao_dict["daohausId"])
        token_list.append(token_dict)

    proposal_list = []
    vote_list = []
    for proposal in dao.get("proposals", []):
        proposal_dict = clean_proposal(proposal, dao_dict["daohausId"])
        proposal_list.append(proposal_dict)

        for vote in proposal.get("votes", []):
            vote_dict = clean_vote(vote, proposal_dict["daohausId"])
            vote_list.append(vote_dict)

    return {
        "dao": dao_dict,
        "members": member_list,
        "tokens": token_list,
        "proposals": proposal_list,
        "votes": vote_list,
    }


def process(dao, network):
    client = boto3.client("s3")
    logger.info(f"starting {dao['id']}")
    key = get_most_recent_key(client, dao["id"])
    if not key:
        return
    d = fetch_daos_file(client, key)
    return handle_dao(d.get("data"), network)


if __name__ == "__main__":

    networks = [
        ("https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus", 1),
        ("https://api.thegraph.com/subgraphs/name/odyssy-automaton/daohaus-xdai", 100),
    ]

    network_dao_list = {
        1: [],
        100: [],
    }

    create_constraints(conn)
    create_indexes(conn)

    for url, network in networks:
        last_id = ""
        all_daos = []
        daos = fetch_daos(url, last_id)
        while len(daos) > 0:
            all_daos.extend(daos)
            logger.debug(f"Total {network} daos fetched: {len(daos)}")
            last_id = daos[-1].get("id")
            daos = fetch_daos(url, last_id)
            logger.info(f"Daos left {len(daos)}, last_id {last_id}")

        network_dao_list[network] = all_daos

    dao_list = []
    member_list = []
    token_list = []
    proposal_list = []
    vote_list = []

    for network, daos in network_dao_list.items():
        with tqdm_joblib(tqdm(desc=f"Handling DAOs for network {network}", total=len(daos))) as progress_bar:
            all_trans = Parallel(n_jobs=-1)(delayed(process)(dao, network) for dao in daos)

        [dao_list.append(x["dao"]) for x in all_trans if x]
        [member_list.extend(x["members"]) for x in all_trans if x]
        [token_list.extend(x["tokens"]) for x in all_trans if x]
        [proposal_list.extend(x["proposals"]) for x in all_trans if x]
        [vote_list.extend(x["votes"]) for x in all_trans if x]

    print(len(dao_list), len(member_list), len(token_list), len(proposal_list), len(vote_list))

    # create DAO nodes
    dao_df = pd.DataFrame(dao_list)
    dao_df.drop_duplicates(subset=["daohausId"], inplace=True)
    list_dao_chunks = split_dataframe(dao_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(list_dao_chunks):
        url = write_df_to_s3(space_batch, BUCKET, f"neo/daohaus/nodes/dao/dao-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_dao_nodes(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/dao/dao-{idx * SPLIT_SIZE}.csv", resource)

    # create token nodes
    token_df = pd.DataFrame(token_list)
    token_node_df = token_df[["address", "symbol", "decimals"]].drop_duplicates(subset=["address"])
    token_dao_chunks = split_dataframe(token_node_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(token_dao_chunks):
        url = write_df_to_s3(space_batch, BUCKET, f"neo/daohaus/nodes/token/token-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_token_nodes(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/token/token-{idx * SPLIT_SIZE}.csv", resource)

    # create token dao relationships
    token_dao_chunks = split_dataframe(token_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(token_dao_chunks):
        url = write_df_to_s3(space_batch, BUCKET, f"neo/daohaus/edges/token/token-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_token_dao_relationships(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/edges/token/token-{idx * SPLIT_SIZE}.csv", resource)

    # create member nodes and dao member relationships
    member_df = pd.DataFrame(member_list)
    member_dao_chunks = split_dataframe(member_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(member_dao_chunks):
        url = write_df_to_s3(
            space_batch, BUCKET, f"neo/daohaus/nodes/member/member-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_wallet_nodes(url, conn)
        merge_member_relationships(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/member/member-{idx * SPLIT_SIZE}.csv", resource)

    # create proposal nodes and dao proposal relationships
    proposal_df = pd.DataFrame(proposal_list)
    proposal_dao_chunks = split_dataframe(proposal_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(proposal_dao_chunks):
        url = write_df_to_s3(
            space_batch, BUCKET, f"neo/daohaus/nodes/proposal/proposal-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_proposal_nodes(url, conn)
        merge_proposal_dao_relationships(url, conn)
        merge_proposal_author_relationships(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/proposal/proposal-{idx * SPLIT_SIZE}.csv", resource)

    # create sponsor relationships
    sponsor_df = proposal_df[proposal_df["sponsor"].notnull()]
    sponsor_dao_chunks = split_dataframe(sponsor_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(sponsor_dao_chunks):
        url = write_df_to_s3(
            space_batch, BUCKET, f"neo/daohaus/edges/sponsor/sponsor-{idx * SPLIT_SIZE}.csv", resource, s3
        )
        merge_proposal_sponsor_relationships(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/edges/sponsor/sponsor-{idx * SPLIT_SIZE}.csv", resource)

    # create vote relationships
    vote_df = pd.DataFrame(vote_list)
    vote_dao_chunks = split_dataframe(vote_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(vote_dao_chunks):
        url = write_df_to_s3(space_batch, BUCKET, f"neo/daohaus/nodes/vote/vote-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_vote_relationships(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/vote/vote-{idx * SPLIT_SIZE}.csv", resource)

    # create summoner relationships
    summoner_df = dao_df[["daohausId", "summoner"]]
    summoner_df = summoner_df[summoner_df["summoner"].notnull()]
    list_dao_chunks = split_dataframe(dao_df, SPLIT_SIZE)

    for idx, space_batch in enumerate(list_dao_chunks):
        url = write_df_to_s3(space_batch, BUCKET, f"neo/daohaus/nodes/dao/dao-{idx * SPLIT_SIZE}.csv", resource, s3)
        merge_summoner_relationships(url, conn)
        set_object_private(BUCKET, f"neo/daohaus/nodes/dao/dao-{idx * SPLIT_SIZE}.csv", resource)

    conn.close()
    logger.info("Finished")
