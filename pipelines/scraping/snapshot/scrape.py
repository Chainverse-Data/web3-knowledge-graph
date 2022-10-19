from ..helpers import Scraper
from ..helpers import tqdm_joblib, get_ens_info, str2bool
from .helpers.query_strings import spaces_query, proposals_query, votes_query
import json
import logging
import tqdm
import os
import re
import multiprocessing
import joblib
import argparse
import math
import time


class SnapshotScraper(Scraper):
    def __init__(self, recent):
        super().__init__("snapshot")
        self.snapshot_url = "https://hub.snapshot.org/graphql"
        self.recent = recent
        self.spaces_query = spaces_query
        self.proposals_query = proposals_query
        self.votes_query = votes_query

        if recent and "last_timestamp" in self.metadata:
            self.cutoff_timestamp = self.metadata["last_timestamp"] - (60 * 60 * 24 * 15)
        else:
            self.cutoff_timestamp = 0

    def get_spaces(self):
        raw_spaces = []
        first = int(re.search("first: \d+", self.spaces_query)[0].split(" ")[1])
        offset = 0
        logging.info("Getting spaces...")
        content = self.post_request(self.snapshot_url, json={"query": self.spaces_query})
        data = json.loads(content)
        results = data["data"]["spaces"]
        while results:
            raw_spaces.extend(results)
            if len(raw_spaces) % 1000 == 0:
                logging.info(f"Current Spaces: {len(raw_spaces)}")
            offset += int(first)
            new_query = self.spaces_query.replace("skip: 0", f"skip: {offset}")
            content = self.post_request(self.snapshot_url, json={"query": new_query})
            data = json.loads(content)
            results = data["data"]["spaces"]
        logging.info(f"Total Spaces: {len(raw_spaces)}")

        with tqdm_joblib(tqdm.tqdm(desc="Getting ENS Data", total=len(raw_spaces))) as progress_bar:
            ens_list = joblib.Parallel(n_jobs=multiprocessing.cpu_count() - 1)(
                joblib.delayed(get_ens_info)(space["id"]) for space in raw_spaces
            )
        for i in range(len(raw_spaces)):
            if ens_list[i] is not None:
                raw_spaces[i]["ens"] = ens_list[i]

        final_spaces = [space for space in raw_spaces if space]
        self.data["spaces"] = final_spaces

    def get_proposals(self):
        raw_proposals = []
        first = int(re.search("first: \d+", self.proposals_query)[0].split(" ")[1])
        offset = 0
        logging.info("Getting proposals...")
        content = self.post_request(self.snapshot_url, json={"query": self.proposals_query})
        data = json.loads(content)
        results = data["data"]["proposals"]
        while results:
            raw_proposals.extend(results)
            if len(raw_proposals) % 1000 == 0:
                print(f"Current Proposals: {len(raw_proposals)}")
            if results[-1].get("created", self.cutoff_timestamp) < self.cutoff_timestamp:
                break
            offset += first
            new_query = self.proposals_query.replace("skip: 0", f"skip: {offset}")
            content = self.post_request(self.snapshot_url, json={"query": new_query})
            data = json.loads(content)
            results = data["data"]["proposals"]

        logging.info(f"Total Proposals: {len(raw_proposals)}")
        self.data["proposals"] = [proposal for proposal in raw_proposals if proposal]

    def scrape_votes(self, proposal_id_list):
        raw_votes = []
        offset = 0
        proposal_id_list = json.dumps(proposal_id_list)
        current_vote_query = self.votes_query.replace("$proposalIDs", f"{proposal_id_list}")
        first = int(re.search("first: \d+", current_vote_query)[0].split(" ")[1])
        results = True
        retries = 0
        while results:
            if isinstance(results, list):
                raw_votes.extend(results)
            offset += first
            new_query = current_vote_query.replace("skip: 0", f"skip: {offset}")
            content = self.post_request(self.snapshot_url, json={"query": new_query})
            if "504: Gateway time-out" in content or "error" in json.loads(content):
                retries += 1
                if retries > 10:
                    break
                logging.info("Gateway timeout, sleeping...")
                time.sleep(5)
                continue
            data = json.loads(content)
            results = data["data"]["votes"]

        return raw_votes

    def get_votes(self):
        proposal_id_list = [proposal["id"] for proposal in self.data["proposals"]]
        logging.info("Getting votes...")
        with tqdm_joblib(
            tqdm.tqdm(desc="Getting Votes Data", total=math.ceil(len(proposal_id_list) / 5))
        ) as progress_bar:
            raw_votes_list = joblib.Parallel(n_jobs=2, backend="threading")(
                joblib.delayed(self.scrape_votes)(proposal_id_list[i : i + 5])
                for i in range(0, len(proposal_id_list), 5)
            )

        raw_votes = [vote for votes in raw_votes_list for vote in votes]
        logging.info(f"Total Votes: {len(raw_votes)}")
        self.data["votes"] = raw_votes

    def run(self):
        self.get_spaces()
        self.get_proposals()
        self.get_votes()

        self.metadata["last_timestamp"] = self.runtime.timestamp()
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Snapshot Scraper")
    parser.add_argument("--recent", type=str2bool, default=True, help="Scrape only recent data")
    args = parser.parse_args()

    scraper = SnapshotScraper(args.recent)
    scraper.run()
