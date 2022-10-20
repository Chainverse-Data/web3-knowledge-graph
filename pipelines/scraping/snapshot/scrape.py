from ..helpers import Scraper
from ..helpers import tqdm_joblib, get_ens_info, str2bool
from .helpers.query_strings import spaces_query, proposals_query, votes_query, proposal_status_query
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
        self.space_limit = 100
        self.proposal_limit = 500
        self.vote_limit = 1000

        self.last_space_offset = 0
        if "last_space_offset" in self.metadata:
            self.last_space_offset = self.metadata["last_space_offset"]

        self.last_proposal_offset = 0
        if "last_proposal_offset" in self.metadata:
            self.last_proposal_offset = self.metadata["last_proposal_offset"]

        self.open_proposals = set()
        if "open_proposals" in self.metadata:
            self.open_proposals = set(self.metadata["open_proposals"])
        
        # I'm not sure what this is for ? Debugging ?
        # if recent and "last_timestamp" in self.metadata:
        #     self.cutoff_timestamp = self.metadata["last_timestamp"] - (60 * 60 * 24 * 15)
        # else:
        #     self.cutoff_timestamp = 0

    def make_api_call(self, query, counter=0, content=None):
        time.sleep(counter)
        if counter > 10:
            logging.error(content)
            raise Exception("Something went wrong while getting the spaces results from the API")
        content = self.post_request(self.snapshot_url, json={"query": query})
        if "504: Gateway time-out" in content:
            return self.make_api_call(query, counter=counter+1, content=content)
        data = json.loads(content)
        if "data" not in data or "error" in data:
            return self.make_api_call(query, counter=counter+1, content=content)
        return data["data"]

    def get_spaces(self):
        logging.info("Getting spaces...")
        raw_spaces = []
        offset = self.last_space_offset
        results = True
        while results:
            self.metadata["last_space_offset"] = offset # Put this here so that we save only the last offset with data
            if len(raw_spaces) % 1000 == 0:
                logging.info(f"Current Spaces: {len(raw_spaces)}")
            query = spaces_query.format(self.space_limit, offset)
            data = self.make_api_call(query)
            results = data["spaces"]
            raw_spaces.extend(results)
            offset += self.space_limit
        logging.info(f"Total Spaces aquired: {len(raw_spaces)}")

        with tqdm_joblib(tqdm.tqdm(desc="Getting ENS Data", total=len(raw_spaces))) as progress_bar:
            ens_list = joblib.Parallel(n_jobs=multiprocessing.cpu_count() - 1)(
                joblib.delayed(get_ens_info)(space["id"]) for space in raw_spaces
            )
        for i in range(len(raw_spaces)):
            if ens_list[i] is not None:
                raw_spaces[i]["ens"] = ens_list[i]

        final_spaces = [space for space in raw_spaces if space]
        self.data["spaces"] = final_spaces
        logging.info(f"Final spaces count: {len(final_spaces)}")

    def get_proposals(self):
        logging.info("Getting proposals...")
        raw_proposals = []
        offset = self.last_proposal_offset
        results = True
        while results:
            self.metadata["last_proposal_offset"] = offset
            if len(raw_proposals) % 1000 == 0:
                logging.info(f"Current Proposals: {len(raw_proposals)}")
            query = proposals_query.format(self.proposal_limit, offset)
            data = self.make_api_call(query)
            results = data["proposals"]
            raw_proposals.extend(results)
            offset += self.proposal_limit
            # I'm not sure what this is for ?
            # if results[-1].get("created", self.cutoff_timestamp) < self.cutoff_timestamp:
            #     break

        proposals = [proposal for proposal in raw_proposals if proposal]
        logging.info(f"Total Proposals: {len(proposals)}")
        self.data["proposals"] = proposals

    def scrape_votes(self, proposal_ids):
        logging.info(f"Scraping votes for proposals: {proposal_ids}")
        raw_votes = []
        offset = 0
        results = True
        while results:
            offset += self.vote_limit
            query = votes_query.format(self.vote_limit, offset, json.dumps(proposal_ids))
            data = self.make_api_call(query)
            results = data["votes"]
            if type(results) != list:
                raise Exception("Something went wrong while getting the data that was not caught correctly!")
            raw_votes.extend(results)
        return raw_votes

    def get_votes(self):
        logging.info("Getting votes...")
        proposal_ids = self.open_proposals + [proposal["id"] for proposal in self.data["proposals"]]
        proposal_ids = list(set(proposal_ids))
        with tqdm_joblib(
            tqdm.tqdm(desc="Getting Votes Data", total=math.ceil(len(proposal_ids) / 5))
        ) as progress_bar:
            raw_votes = joblib.Parallel(n_jobs=2, backend="threading")(
                joblib.delayed(self.scrape_votes)(proposal_ids[i : i + 5])
                for i in range(0, len(proposal_ids), 5)
            )

        votes = [vote for votes in raw_votes for vote in votes]
        logging.info(f"Total Votes: {len(votes)}")
        self.data["votes"] = votes

    def get_proposals_status(self):
        proposal_statuses = []
        for proposal_id in self.open_proposals:
            query = proposal_status_query.format(proposal_id)
            data = self.make_api_call(query)
            if len(data["proposals"]) == 0:
                raise Exception(f"Something unexpected happened with proposal id: {proposal_id}")
            proposal_statuses += data["proposals"]
        open_proposals = [proposal["id"] for proposal in proposal_statuses if proposal["state"] != "closed"]
        open_proposals += [proposal["id"] for proposal in self.data["proposals"] if proposal["state"] != "closed"]
        self.open_proposals = list(set(open_proposals))
        self.metadata["open_proposals"] = self.open_proposals


    def run(self):
        self.get_spaces()
        self.get_proposals()
        self.get_votes()
        self.get_proposals_status()

        # Same not sure what this is for
        # self.metadata["last_timestamp"] = self.runtime.timestamp()
        self.save_metadata()
        self.save_data()


if __name__ == "__main__":
    # So first, this should be an ENV var, that will make it much easier
    # Second, scrapping recent data is the default, only reinit should be flaged and that's handled through the metadata reset.
    # parser = argparse.ArgumentParser(description="Snapshot Scraper")
    # parser.add_argument("--recent", type=str2bool, default=True, help="Scrape only recent data")
    # args = parser.parse_args()

    scraper = SnapshotScraper()
    scraper.run()
