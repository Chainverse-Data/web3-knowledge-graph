import logging
from ..helpers import Processor
from .cyphers import TwitterCyphers
from datetime import datetime, timedelta
import os
import re
import requests
import json
import time


class TwitterPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterCyphers()
        super().__init__("twitter")
        self.cutoff = datetime.now() - timedelta(days=20)
        self.full_job = int(os.environ.get("FULL_TWITTER_JOB", 0))

        self.batch_size = 100
        self.split_size = 10000
        self.bad_handles = set()
        self.bearer_tokens = os.environ.get("TWITTER_BEARER_TOKEN").split(",")
        self.bearer_token_index = 0
        
    def filter_batch(self, batch):
        rex = re.compile("^[A-Za-z0-9_]{1,15}$")
        new_batch = []
        for x in batch:
            if not x or not bool(rex.match(x)):
                self.bad_handles.add(x)
            else:
                new_batch.append(x)
        return new_batch

    def get_user_response(self, batch, retries=0):
        if retries > 10:
            return {"data": []}

        if len(batch) == 0:
            return {"data": []}

        twitter_handles_batch = ",".join(batch)
        headers = {
            "Authorization": f"Bearer {self.bearer_tokens[self.bearer_token_index]}",
        }
        x = requests.get(
            f"https://api.twitter.com/2/users/by?usernames={twitter_handles_batch}&user.fields=description,id,location,name,public_metrics,verified,profile_image_url,url&expansions=pinned_tweet_id&tweet.fields=geo,lang",
            headers=headers,
        )
        resp = json.loads(x.text)
        head = dict(x.headers)
        if "data" not in resp and resp["title"] == "Too Many Requests":
            end_time = head["x-rate-limit-reset"]
            epoch_time = int(time.time())
            time_to_wait = int(end_time) - epoch_time
            logging.warning(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            return self.get_user_response(batch, retries=retries + 1)

        self.bearer_token_index += 1
        self.bearer_token_index %= len(self.bearer_tokens)

        return resp

    def get_twitter_nodes_data(self):
        if self.full_job == 2:
            twitter_handles = self.cyphers.get_all_twitter()
        elif self.full_job == 1:
            twitter_handles = self.cyphers.get_all_empty_twitter()
        else:
            twitter_handles = self.cyphers.get_recent_empty_twitter(self.cutoff)
        logging.info(f"Found {len(twitter_handles)} twitter handles")

        users = []
        twitter_ids = {}
        for idx in range(0, len(twitter_handles), self.batch_size):
            twitter_handles_batch = twitter_handles[idx : idx + self.batch_size]
            batch = self.filter_batch(twitter_handles_batch)
            set_items = set(batch)
            resp = self.get_user_response(batch)
            data = resp.get("data", [])
            logging.info(f"Got {len(data)} users from the API")
            for idx, user in enumerate(data):
                tmp = {
                    "name": self.cyphers.sanitize_text(user["name"]),
                    "handle": user["username"].lower(),
                    "bio": self.cyphers.sanitize_text(user["description"]),
                    "verified": user["verified"],
                    "userId": user["id"],
                    "followerCount": user["public_metrics"]["followers_count"],
                    "profileImageUrl": user["profile_image_url"],
                    "website": user.get("url", ""),
                    "location": user.get("location", ""),
                    "language": user.get("language", ""),
                }
                if tmp["handle"] in set_items:
                    set_items.remove(tmp["handle"])
                users.append(tmp)
                pinned_id = user.get("pinned_tweet_id", -1)
                if pinned_id != -1:
                    twitter_ids[pinned_id] = idx
            includes = resp.get("includes", {"tweets": []})
            for idx, entry in enumerate(includes["tweets"]):
                users[twitter_ids[entry["id"]]]["language"] = entry["lang"]
            self.bad_handles.update(set_items)

        logging.info(f"Grabbed the data of {len(users)} users from the API")
        node_info_urls = self.save_json_as_csv(users, f"processor_twitter_data_{self.asOf}")
        self.cyphers.add_twitter_node_info(node_info_urls)

        # add trash labels to bad handle nodes
        bad_handles = [{"handle": x} for x in self.bad_handles]
        logging.info(f"Found {len(bad_handles)} bad handles")
        trash_info_urls = self.save_json_as_csv(bad_handles, f"processor_twitter_trash_{self.asOf}")
        self.cyphers.add_trash_labels(trash_info_urls)

    def clean_twitter_nodes(self):
        logging.info("Cleaning Twitter nodes that don't have the Account label")
        self.cyphers.clean_twitter_nodes()

    def run(self):
        self.get_twitter_nodes_data()


if __name__ == "__main__":
    processor = TwitterPostProcess()
    processor.run()
