import boto3
import json
import pandas as pd
from dotenv import load_dotenv
import sys
sys.path.append(".")

from helpers.s3 import write_df_to_s3

load_dotenv()
resource = boto3.resource("s3")
s3 = boto3.client("s3")
BUCKET = "chainverse"


all_dao_ids = set()
all_files = set()


def iterate_bucket_items(bucket):
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket)

    for page in page_iterator:
        if page["KeyCount"] > 0:
            for item in page["Contents"]:
                if item["Key"].startswith("daohaus") and item["LastModified"].year < 2022:
                    split_result = item["Key"].split("/")
                    if split_result[1] not in all_dao_ids:
                        all_dao_ids.add(split_result[1])
                        yield item


if __name__ == "__main__":

    counter = 0
    all_daos = []
    all_proposals = []
    all_members = []
    all_tokens = []

    for item in iterate_bucket_items("chainverse"):
        current_file_name = item["Key"]

        content_object = s3.get_object(Bucket="chainverse", Key=current_file_name)
        data = content_object["Body"].read().decode("utf-8")
        json_data = json.loads(data)["data"]

        chain_id = current_file_name.split("_")[2].split(".")[0]
        if chain_id == "100":
            chain = "gnosis"
        elif chain_id == "1":
            chain = "mainnet"
        else:
            print(chain)
            continue

        members = json_data.pop("members", None)
        assert members != None
        proposals = json_data.pop("proposals", None)
        assert members != None
        tokens = json_data.pop("tokens", None)
        assert tokens != None

        counter += 1
        if counter % 100 == 0:
            print(f"Count: {counter}")

        json_data["chain"] = chain
        json_data["chain_id"] = chain_id
        all_daos.append(json_data)

        for idx, entry in enumerate(members):
            members[idx]["chain"] = chain
            members[idx]["chain_id"] = chain_id
        all_members.extend(members)

        for idx, entry in enumerate(proposals):
            proposals[idx]["chain"] = chain
            proposals[idx]["chain_id"] = chain_id
        all_proposals.extend(proposals)

        for idx, entry in enumerate(tokens):
            tokens[idx]["chain"] = chain
            tokens[idx]["chain_id"] = chain_id
        all_tokens.extend(tokens)

    dao_df = pd.DataFrame(all_daos)
    proposal_df = pd.DataFrame(all_proposals)
    member_df = pd.DataFrame(all_members)
    token_df = pd.DataFrame(all_tokens)

    write_df_to_s3(dao_df, BUCKET, "neo/daohaus/raw/dao.csv", resource, s3, "private")
    write_df_to_s3(proposal_df, BUCKET, "neo/daohaus/raw/proposal.csv", resource, s3, "private")
    write_df_to_s3(member_df, BUCKET, "neo/daohaus/raw/member.csv", resource, s3, "private")
    write_df_to_s3(token_df, BUCKET, "neo/daohaus/raw/token.csv", resource, s3, "private")

