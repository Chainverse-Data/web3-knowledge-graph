import logging
from ..helpers import Processor
from .cyphers import TwitterFollowerCyphers
from datetime import datetime, timedelta
import os
import re
import requests
import json
import time


class TwitterFollowPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterFollowerCyphers()
        super().__init__("twitter")
        self.cutoff = datetime.now() - timedelta(days=20)

        self.items = []
        self.headers = {
            "Authorization": f"Bearer {os.environ.get('TWITTER_BEARER_TOKEN')}",
        }

    def twitter_api_call(self, url, retries=0):
        if retries > 10:
            return {"data": []}

        x = requests.get(
            url,
            headers=self.headers,
        )
        resp = json.loads(x.text)
        head = dict(x.headers)
        if "data" not in resp and resp["title"] == "Too Many Requests":
            end_time = head["x-rate-limit-reset"]
            epoch_time = int(time.time())
            time_to_wait = int(end_time) - epoch_time
            logging.warning(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            return self.get_user_response(url, retries=retries + 1)

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

    def get_followers(self):
        logging.info("Getting followers")
        follower_url = "https://api.twitter.com/2/users/{}/followers?max_results=1000{}&user.fields=username"
        results = []
        for entry in self.items:
            items = self.handle_user(entry, follower_url)
            for follower in items:
                results.append(
                    {
                        "handle": entry.get("handle").lower(),
                        "follower": follower.get("username").lower(),
                    }
                )
        return results

    def get_following(self):
        logging.info("Getting following")
        following_url = "https://api.twitter.com/2/users/{}/following?max_results=1000{}&user.fields=username"
        results = []
        for entry in self.items:
            items = self.handle_user(entry, following_url)
            for following in items:
                results.append(
                    {
                        "handle": following.get("username").lower(),
                        "follower": entry.get("handle").lower(),
                    }
                )
        return results

    def handle_user(self, user, url):
        token = None
        results = []
        while True:
            if token:
                cur_url = url.format(user.get("id"), f"&pagination_token={token}")
            else:
                cur_url = url.format(user.get("id"), "")
            resp = self.twitter_api_call(cur_url)
            results.extend(resp.get("data"))
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
        self.get_twitter_handles()
        followers = self.get_followers()
        following = self.get_following()
        self.handle_ingestion(followers + following)


if __name__ == "__main__":
    processor = TwitterFollowPostProcess()
    processor.run()
