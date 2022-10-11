from ..helpers import Ingestor
from .cyphers import SnapshotCyphers
import json
import pandas
from typing import Dict, List, Any


class SnapshotIngestor(Ingestor):
    def __init__(self):
        self.cyphers = SnapshotCyphers()
        super().__init__("snapshot")

    def ingest_spaces(self):
        print("Ingesting spaces...")
        space_data = self.process_spaces()
        self.ingest_data.update(space_data)

        # add space nodes
        urls = self.s3.save_json_as_csv(space_data["spaces"], self.bucket_name, f"ingestor_spaces_{self.asOf}")
        self.cyphers.create_or_merge_spaces(urls)

        # add twitter nodes, twitter-space relationships
        twitter_df = pandas.DataFrame(space_data["spaces"])[
            ["snapshotId", "handle", "twitterProfileUrl"]
        ].drop_duplicates(subset=["handle"])
        twitter_df = twitter_df[twitter_df["handle"] != ""]
        twitter_dict = twitter_df.to_dict("records")
        urls = self.s3.save_json_as_csv(twitter_dict, self.bucket_name, f"ingestor_twitter_{self.asOf}")
        self.cyphers.create_or_merge_twitter(urls)
        self.cyphers.link_space_twitter(urls)

        # add token nodes
        urls = self.s3.save_json_as_csv(space_data["tokens"], self.bucket_name, f"ingestor_tokens_{self.asOf}")
        self.cyphers.create_or_merge_tokens(urls)

        # add strategy relationships (token-space)
        urls = self.s3.save_json_as_csv(
            space_data["strategy_relationships"], self.bucket_name, f"ingestor_strategies_{self.asOf}"
        )
        self.cyphers.link_strategies(urls)

        # add ens nodes, ens relationships, space-alias relationships
        urls = self.s3.save_json_as_csv(space_data["ens"], self.bucket_name, f"ingestor_ens_{self.asOf}")
        self.cyphers.create_or_merge_ens(urls)
        self.cyphers.link_ens(urls)
        self.cyphers.link_space_alias(urls)

        # add member wallet nodes, member-space relationships
        urls = self.s3.save_json_as_csv(space_data["members"], self.bucket_name, f"ingestor_members_{self.asOf}")
        self.cyphers.create_or_merge_wallets(urls)
        self.cyphers.link_member_spaces(urls)

        # add admin wallet nodes, admin-space relationships
        urls = self.s3.save_json_as_csv(space_data["admins"], self.bucket_name, f"ingestor_admins_{self.asOf}")
        self.cyphers.create_or_merge_wallets(urls)
        self.cyphers.link_admin_spaces(urls)

    def ingest_proposals(self):
        print("Ingesting proposals...")
        proposal_data = self.process_proposals()
        self.ingest_data.update(proposal_data)

        # add proposal nodes, proposal-space relationships, proposal author wallet nodes, proposal-author relationships
        urls = self.s3.save_json_as_csv(proposal_data["proposals"], self.bucket_name, f"ingestor_proposals_{self.asOf}")
        self.cyphers.create_or_merge_proposals(urls)
        self.cyphers.link_proposal_spaces(urls)
        self.cyphers.create_or_merge_wallets(urls)
        self.cyphers.link_proposal_authors(urls)

    def ingest_votes(self):
        print("Ingesting votes...")
        vote_data = self.process_votes()
        self.ingest_data.update(vote_data)

        wallet_dict = [
            {"address": wallet}
            for wallet in set([x["voter"] for x in vote_data["votes"]])
            if wallet != "" and wallet is not None
        ]

        # add vote wallet nodes
        urls = self.s3.save_json_as_csv(wallet_dict, self.bucket_name, f"ingestor_wallets_{self.asOf}")
        self.cyphers.create_or_merge_wallets(urls)

        # add vote relationships (proposal-wallet)
        urls = self.s3.save_json_as_csv(vote_data["votes"], self.bucket_name, f"ingestor_votes_{self.asOf}")
        self.cyphers.link_votes(urls)

    def process_spaces(self) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        space_data = {
            "spaces": [],
            "strategy_list": [],
            "ens": [],
            "members": [],
            "admins": [],
            "tokens": [],
            "strategy_relationships": [],
        }
        for entry in self.scraper_data["spaces"]:
            current_dict = {}
            current_dict["snapshotId"] = entry["id"]
            current_dict[
                "profileUrl"
            ] = f"https://cdn.stamp.fyi/space/{current_dict['snapshotId']}?s=160&cb=a1b40604488c19a1"
            current_dict["name"] = entry["name"]
            current_dict["about"] = self.sanitize(entry.get("about", ""))
            current_dict["chainId"] = entry.get("network", "")
            current_dict["symbol"] = entry.get("symbol", "")
            current_dict["handle"] = entry.get("twitter", "")
            current_dict["handle"] = current_dict["handle"] if current_dict["handle"] else ""
            current_dict["twitterProfileUrl"] = "https://twitter.com/" + current_dict["handle"]

            filters = entry.get("filters", {})
            current_dict["onlyMembers"] = filters.get("members", False)
            current_dict["minScore"] = filters.get("minScore", -1)

            if "strategies" in entry and entry["strategies"]:
                for strategy in entry["strategies"]:
                    space_data["strategy_list"].append({"space": entry["id"], "strategy": strategy})

            for member in entry["members"]:
                space_data["members"].append({"space": entry["id"], "address": member.lower()})

            for admin in entry["admins"]:
                space_data["admins"].append({"space": entry["id"], "address": admin.lower()})

            if "ens" in entry:
                ens = entry["ens"]
                space_data["ens"].append(
                    {
                        "name": entry["id"],
                        "owner": ens["owner"].lower(),
                        "tokenId": ens["token_id"],
                        "txHash": ens["trans"]["hash"],
                        "contractAddress": ens["trans"]["rawContract"]["address"],
                    }
                )

            space_data["spaces"].append(current_dict)

        space_data["ens"] = pandas.DataFrame(space_data["ens"]).drop_duplicates(["name"]).to_dict("records")

        for item in space_data["strategy_list"]:
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
                space_data["tokens"].append(token_dict)
                space_data["strategy_relationships"].append(current_dict)
            except:
                continue

            return space_data

    def process_proposals(self) -> Dict[str, List[Dict[str, Any]]]:
        proposal_data = {"proposals": []}
        for entry in self.scraper_data["proposals"]:
            current_dict = {}
            current_dict["snapshotId"] = entry["id"]
            current_dict["ipfsCID"] = entry["ipfs"]
            current_dict["address"] = entry["author"].lower() or ""
            current_dict["createdAt"] = entry["created"] or 0
            current_dict["type"] = entry["type"] or -1
            current_dict["spaceId"] = entry["space"]["id"]

            current_dict["title"] = self.sanitize(entry["title"])
            current_dict["text"] = self.sanitize(entry["body"])

            choices = self.sanitize(json.dumps(entry["choices"]))

            current_dict["choices"] = choices
            current_dict["startDt"] = entry["start"] or 0
            current_dict["endDt"] = entry["end"] or 0
            current_dict["state"] = entry["state"] or ""
            current_dict["link"] = entry["link"].strip() or ""

            proposal_data["proposals"].append(current_dict)

        proposal_df = (
            pandas.DataFrame(proposal_data["proposals"]).drop_duplicates("snapshotId").dropna(subset=["address"])
        )
        proposal_data["proposals"] = proposal_df.to_dict("records")
        return proposal_data

    def process_votes(self) -> Dict[str, List[Dict[str, Any]]]:
        vote_data = {"votes": []}
        for entry in self.scraper_data["votes"]:
            current_dict = {}
            current_dict["id"] = entry["id"]
            current_dict["voter"] = entry["voter"].lower() or ""
            current_dict["votedAt"] = entry["created"]
            current_dict["ipfs"] = entry["ipfs"]

            try:
                current_dict["choice"] = self.sanitize(json.dumps(entry["choice"]))
                current_dict["proposalId"] = entry["proposal"]["id"]
                current_dict["spaceId"] = entry["space"]["id"]
            except:
                continue

            vote_data["votes"].append(current_dict)

        vote_data["votes"] = pandas.DataFrame(vote_data["votes"]).drop_duplicates("id").to_dict("records")
        return vote_data

    def run(self):
        self.ingest_spaces()
        self.ingest_proposals()
        self.ingest_votes()


if __name__ == "__main__":
    ingestor = SnapshotIngestor()
    ingestor.run()
