import json
from tqdm import tqdm as tqdm
import argparse
import time
import boto3
from datetime import datetime, timedelta
from joblib import Parallel, delayed
from pathlib import Path
import sys
import os
import re
import multiprocessing
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[1]))

from helpers.util import str2bool, run_snapshot_query, expand_path, get_ens, tqdm_joblib

load_dotenv()
provider = "https://eth-mainnet.alchemyapi.io/v2/" + str(os.environ.get("ALCHEMY_API_KEY"))


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
}
PROXIES = []
if Path("data/proxies.txt").is_file():
    with open("data/proxies.txt", "r") as f:
        PROXIES_raw = [x.strip() for x in f.readlines()]
        for entry in PROXIES_raw:
            split = entry.split(":")
            new_entry = {
                "http": f"http://{split[2]}:{split[3]}@{split[0]}:{split[1]}",
                "https": f"http://{split[2]}:{split[3]}@{split[0]}:{split[1]}",
            }
            PROXIES.append(new_entry)


class SnapshotScrapper:
    def __init__(self, s3, query_dir, now, recent):
        self.s3 = s3
        self.now = now
        query_dir = Path(query_dir)
        with open(query_dir / "spaces.txt", "r") as file:
            self.spaces_query = file.read()

        with open(query_dir / "proposals.txt", "r") as file:
            self.proposals_query = file.read()

        with open(query_dir / "votes.txt", "r") as file:
            self.votes_query = file.read()

        self.recent = recent

    def parse(self):
        raw_spaces = self.get_spaces()
        file_name = f"snapshot/spaces/{self.now.strftime('%m-%d-%Y')}/spaces.json"
        s3object = self.s3.Object("chainverse", file_name)
        s3object.put(Body=(bytes(json.dumps(raw_spaces).encode("UTF-8"))))
        print(f"Spaces saved to s3 at {file_name}")

        raw_proposals = self.get_proposals()
        file_name = f"snapshot/proposals/{self.now.strftime('%m-%d-%Y')}/proposals.json"
        s3object = self.s3.Object("chainverse", file_name)
        s3object.put(Body=(bytes(json.dumps(raw_proposals).encode("UTF-8"))))
        print(f"Proposals saved to s3 at {file_name}")

        return raw_spaces, raw_proposals

    def get_spaces(self):
        raw_spaces = []
        first = int(re.search("first: \d+", self.spaces_query)[0].split(" ")[1])
        offset = 0
        print("Getting spaces...")
        results = json.loads(run_snapshot_query(self.spaces_query))["data"]["spaces"]
        while results:
            raw_spaces.extend(results)
            if len(raw_spaces) % 1000 == 0:
                print(f"Current Spaces: {len(raw_spaces)}")
            offset += int(first)
            new_query = self.spaces_query.replace("skip: 0", f"skip: {offset}")
            results = json.loads(run_snapshot_query(new_query))["data"]["spaces"]
        print(f"Total Spaces: {len(raw_spaces)}")

        with tqdm_joblib(tqdm(desc="Getting ENS Data", total=len(raw_spaces))) as progress_bar:
            ens_list = Parallel(n_jobs=multiprocessing.cpu_count() - 1)(
                delayed(get_ens)(space, provider) for space in raw_spaces
            )
        for i in range(len(raw_spaces)):
            if ens_list[i] is not None:
                raw_spaces[i]["ens"] = ens_list[i]

        return [space for space in raw_spaces if space]

    def get_proposals(self):
        raw_proposals = []
        first = int(re.search("first: \d+", self.proposals_query)[0].split(" ")[1])
        offset = 0
        if self.recent:
            old_time = self.now - timedelta(days=15)
            cutoff_timestamp = int(time.mktime(old_time.timetuple()))
        else:
            cutoff_timestamp = 0
        print("Getting proposals...")
        results = json.loads(run_snapshot_query(self.proposals_query))["data"]["proposals"]
        while results:
            raw_proposals.extend(results)
            if len(raw_proposals) % 1000 == 0:
                print(f"Current Proposals: {len(raw_proposals)}")
            try:
                if results[-1]["created"] < cutoff_timestamp:
                    break
            except:
                pass
            offset += first
            new_query = self.proposals_query.replace("skip: 0", f"skip: {offset}")
            results = json.loads(run_snapshot_query(new_query))["data"]["proposals"]
        print(f"Total Proposals: {len(raw_proposals)}")

        return [proposal for proposal in raw_proposals if proposal]


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--recent", type=str2bool, default=True)
    parser.add_argument("--query_dir", type=expand_path, default=str(Path(__file__).resolve().parent / "query_files"))

    flags = parser.parse_args()

    s3 = boto3.resource("s3")
    now = datetime.now()
    scrapper = SnapshotScrapper(s3, flags.query_dir, now, flags.recent)
    existing_votes = []
    raw_spaces, raw_proposals = scrapper.parse()
    print("Done")
