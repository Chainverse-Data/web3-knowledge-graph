import json
from tqdm import tqdm as tqdm
import argparse
import time
import boto3
from datetime import datetime, timedelta
from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parent))
sys.path.append(str(Path(__file__).resolve().parents[1]))
import os
import re
from queue import Queue
from threading import Thread
import multiprocessing
from helpers.util import str2bool, run_snapshot_query, expand_path
from dotenv import load_dotenv

load_dotenv()

class WriteThread(Thread):
    def __init__(self, s3, now, proposals, existing_votes, queue: Queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue = queue
        self.s3 = s3
        self.now = now
        self.proposals = set([proposal["id"] for proposal in proposals])
        self.existing_votes = existing_votes

    def run(self):
        existing_votes = self.existing_votes
        old_count = 0
        old_count_5k = len(existing_votes) // 2000000
        file_name = f"snapshot/votes/{self.now.strftime('%m-%d-%Y')}/votes.json"

        while True:
            votes_list, proposal_id_list = self.queue.get()
            if votes_list is None and not isinstance(votes_list, list):
                print(f"Total Votes: {len(existing_votes)}")
                s3object = self.s3.Object("chainverse", file_name)
                s3object.put(Body=(bytes(json.dumps(existing_votes).encode("UTF-8"))))
                print(f"Votes saved to s3 at {file_name}")
                break

            self.proposals = self.proposals - set(proposal_id_list)
            print(f"{len(self.proposals)} proposals left to write")

            existing_votes = votes_list + existing_votes
            if len(existing_votes) // 100000 != old_count:
                old_count = len(existing_votes) // 100000
                print(f"Current Votes: {len(existing_votes)}")

            if len(existing_votes) // 2000000 != old_count_5k:
                old_count_5k = len(existing_votes) // 2000000
                s3object = self.s3.Object("chainverse", file_name)
                s3object.put(Body=(bytes(json.dumps(existing_votes).encode("UTF-8"))))
                print(f"Votes saved to s3 at {file_name}")
                new_dict = [{"id": proposal} for proposal in self.proposals]
                with open("data/remaining_proposals.json", "w") as file:
                    json.dump(new_dict, file)


class ScrapeThread(Thread):
    def __init__(self, proposals, vote_query, queue: Queue, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proposals = [proposal["id"] for proposal in proposals]
        self.votes_query = vote_query
        self.queue = queue
        first = int(re.search("first: \d+", self.votes_query)[0].split(" ")[1])
        self.first = first

    def scrape(self, proposal_id_list):
        raw_votes = []
        offset = 0
        proposal_id_list = json.dumps(proposal_id_list)
        current_vote_query = self.votes_query.replace("$proposalIDs", f"{proposal_id_list}")
        results = json.loads(run_snapshot_query(current_vote_query))["data"]["votes"]
        while results:
            raw_votes.extend(results)
            offset += self.first
            new_query = current_vote_query.replace("skip: 0", f"skip: {offset}")
            results = json.loads(run_snapshot_query(new_query))["data"]["votes"]

        return raw_votes

    def run(self):
        idx = 0
        while idx < len(self.proposals):
            try:
                proposal_id_list = self.proposals[idx : idx + 5]
                votes_list = ScrapeThread.scrape(self, proposal_id_list)
                if votes_list:
                    self.queue.put((votes_list, proposal_id_list))
            except Exception as e:
                print(f"ScrapeThread Exception: {e}")
            idx += 5


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query_dir", type=expand_path, default=str(Path(__file__).resolve().parent / "query_files"))

    flags = parser.parse_args()

    s3 = boto3.resource("s3")
    now = datetime.now()

    file_name = f"snapshot/proposals/{now.strftime('%m-%d-%Y')}/proposals.json"
    s3object = s3.Object("chainverse", file_name)
    file_content = s3object.get()["Body"].read().decode("utf-8")
    raw_proposals = json.loads(file_content)
    print(f"Total Loaded Proposals: {len(raw_proposals)}")

    existing_votes = []

    query_dir = Path(flags.query_dir)
    with open(query_dir / "votes.txt") as file:
        vote_query = file.read()

    WORKER_THREADS = multiprocessing.cpu_count()
    queue = Queue()

    print("Getting votes...")
    write_thread = WriteThread(s3, now, raw_proposals, existing_votes, queue)
    write_thread.start()

    worker_threads = []
    chunk_size = len(raw_proposals) // WORKER_THREADS
    for i in range(0, len(raw_proposals), chunk_size):
        chunk = raw_proposals[i : i + chunk_size]
        worker_threads.append(ScrapeThread(chunk, vote_query, queue))

    for thread in worker_threads:
        thread.start()

    for thread in worker_threads:
        thread.join()

    # Signal end of jobs to write thread
    queue.put((None, None))

    print("Done.")
    write_thread.join()