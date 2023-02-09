import logging
from ..helpers import Processor
from .cyphers import TwitterFollowerCyphers
from datetime import datetime, timedelta
import os
import re
import requests
import json
import time
import tqdm


class TwitterFollowPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterFollowerCyphers()
        super().__init__("twitter")
        self.cutoff = datetime.now() - timedelta(days=20)

        self.items = []
        self.bearer_tokens = os.environ.get("TWITTER_BEARER_TOKEN").split(",")
        self.i = 0
        self.metadata["following"] = self.metadata.get("following", [])
        self.metadata["followers"] = self.metadata.get("followers", [])

    def twitter_api_call(self, url, retries=0):
        if retries > 10:
            return {"data": []}
        headers = {
            "Authorization": f"Bearer {self.bearer_tokens[self.i]}",
        }
        x = requests.get(
            url,
            headers=headers,
        )
        resp = json.loads(x.text)
        head = dict(x.headers)
        if "data" not in resp and "title" not in resp:
            return resp

        if "data" not in resp and resp["title"] == "Too Many Requests":
            end_time = head["x-rate-limit-reset"]
            epoch_time = int(time.time())
            time_to_wait = int(end_time) - epoch_time
            logging.warning(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            return self.twitter_api_call(url, retries=retries + 1)

        self.i += 1
        self.i %= len(self.bearer_tokens)
        return resp

    def get_twitter_handles(self):
        logging.info("Getting twitter handles")
        results = []
        results.extend(self.cyphers.get_wallet_alias_handles())
        results.extend(self.cyphers.get_wallet_handles())
        results.extend(self.cyphers.get_entity_alias_handles())
        results.extend(self.cyphers.get_entity_handles())
        results.extend(self.cyphers.get_token_handles())

        for entry in results:
            self.items.append({"id": entry.get("userId"), "handle": entry.get("handle")})
        logging.info(f"Found {len(self.items)} twitter handles")

    def get_high_rep_handles(self):
        results = self.cyphers.get_high_rep_handles()
        for entry in results:
            self.items.append(
                {"id": entry.get("t.userId"), "handle": entry.get("t.handle"), "rep": entry.get("reputation")}
            )

        logging.info(f"Found {len(self.items)} twitter handles")

    def get_followers(self):
        logging.info("Getting followers")
        follower_url = "https://api.twitter.com/2/users/{}/followers?max_results=1000{}&user.fields=username"
        results = []
        if self.metadata.get("followers", None):
            results = self.s3.load_csv(self.bucket_name, "twitter_followers.csv").to_dict("records")
            logging.info(f"Loaded {len(results)} followers from S3")

        for idx, entry in tqdm.tqdm(enumerate(self.items), total=len(self.items), desc="Getting followers"):
            if entry.get("id") in self.metadata["followers"]:
                continue
            items = self.handle_user(entry, follower_url)
            for follower in items:
                results.append(
                    {
                        "handle": entry.get("handle").lower(),
                        "follower": follower.get("username").lower(),
                    }
                )
            if idx % 100:  # Save every 500
                self.s3.save_full_json_as_csv(results, self.bucket_name, "twitter_followers")
                self.save_metadata()
            self.metadata["followers"].append(entry.get("id"))
        return results

    def get_following(self):
        logging.info("Getting following")
        following_url = "https://api.twitter.com/2/users/{}/following?max_results=1000{}&user.fields=username"
        results = []
        if self.metadata.get("following", None):
            results = self.s3.load_csv(self.bucket_name, "twitter_following.csv").to_dict("records")
            logging.info(f"Loaded {len(results)} following from S3")

        for idx, entry in tqdm.tqdm(enumerate(self.items), total=len(self.items), desc="Getting following"):
            if entry.get("id") in self.metadata["following"]:
                continue
            items = self.handle_user(entry, following_url)
            for following in items:
                results.append(
                    {
                        "handle": following.get("username").lower(),
                        "follower": entry.get("handle").lower(),
                    }
                )
            self.metadata["following"].append(entry.get("id"))
            if idx % 500:
                self.s3.save_full_json_as_csv(results, self.bucket_name, "twitter_following")
                self.save_metadata()
        return results

    def handle_user(self, user, url):
        token = None
        results = []
        if user.get("id") is None:
            return results
        while True:
            if token:
                cur_url = url.format(user.get("id"), f"&pagination_token={token}")
            else:
                cur_url = url.format(user.get("id"), "")
            resp = self.twitter_api_call(cur_url)
            if "data" not in resp:
                break
            results.extend(resp.get("data"))
            if len(results) >= 5000:
                break
            meta = resp.get("meta", {})
            if meta.get("next_token", None):
                token = meta.get("next_token")
            else:
                break

        return results

    def handle_ingestion(self, data):
        logging.info("Ingesting data")
        all_handles = set()
        for entry in data:
            all_handles.add(entry.get("handle"))
            all_handles.add(entry.get("follower"))
        all_handle_list = []
        for handle in all_handles:
            all_handle_list.append({"handle": handle, "profileUrl": f"https://twitter.com/{handle}"})

        handle_urls = self.s3.save_json_as_csv(all_handle_list, self.bucket_name, "twitter_follow_handles.csv")
        self.cyphers.create_or_merge_twitter_nodes(handle_urls)

        follower_urls = self.s3.save_json_as_csv(data, self.bucket_name, "twitter_followers.csv")
        self.cyphers.merge_follow_relationships(follower_urls)

    def run(self):
        self.get_high_rep_handles()
        # followers = self.get_following()
        following = self.get_followers()
        self.handle_ingestion(followers + following)


if __name__ == "__main__":
    processor = TwitterFollowPostProcess()
    processor.run()
