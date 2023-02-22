import logging
from ..helpers import Processor
from .cyphers import TwitterFollowersCyphers
from datetime import datetime, timedelta
import os
import time
from tqdm import tqdm

DEBUG = os.environ.get("DEBUG", False)

class TwitterFollowersPostProcess(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterFollowersCyphers()
        super().__init__("twitter")
        self.cutoff = datetime.now() - timedelta(days=30)
        self.runtime = datetime.now()
        self.chunk_size = 10000
        self.data = {}
        self.items = []
        self.bearer_tokens = os.environ.get("TWITTER_BEARER_TOKEN").split(",")
        self.current_bearer_token_index = 0
        self.metadata["followers"] = self.metadata.get("followers", {})
        for account in self.metadata["followers"]:
            self.metadata["followers"][account] = datetime.fromtimestamp(self.metadata["followers"][account])

    def twitter_api_call(self, url, userId, pagination_token=None, results=[], retries=0):
        if retries > 10:
            return None
        headers = {
            "Authorization": f"Bearer {self.bearer_tokens[self.current_bearer_token_index]}",
        }

        if pagination_token:
            cur_url = url.format(userId, f"&pagination_token={pagination_token}")
        else:
            cur_url = url.format(userId, "")

        response = self.get_request(cur_url, headers=headers, decode=False, json=False, ignore_retries=True)
        data = response.json()

        if "data" not in data and "title" not in data:
            return results

        if data["title"] == "Too Many Requests":
            head = dict(response.headers)
            end_time = head["x-rate-limit-reset"]
            epoch_time = int(time.time())
            time_to_wait = int(end_time) - epoch_time
            logging.warning(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            return self.twitter_api_call(url, userId, pagination_token=pagination_token, results=results, retries=retries + 1)

        results.extend(data.get("data"))
        
        metadata = data.get("meta", {})
        if metadata.get("next_token", None):
            pagination_token = metadata.get("next_token")
        else:
            return results

        self.current_bearer_token_index += 1
        self.current_bearer_token_index %= len(self.bearer_tokens)
        return self.twitter_api_call(url, userId, pagination_token=pagination_token, results=results, retries=retries)

    def get_twitter_handles(self):
        logging.info("Getting twitter handles")
        results = []
        results.extend(self.cyphers.get_wallet_alias_handles())
        results.extend(self.cyphers.get_wallet_handles())
        results.extend(self.cyphers.get_entity_alias_handles())
        results.extend(self.cyphers.get_entity_handles())
        results.extend(self.cyphers.get_token_handles())

        if DEBUG:
            results = results[:10]
        for entry in results:
            self.items.append({"userId": entry.get("userId"), "handle": entry.get("handle")})
        logging.info(f"Found {len(self.items)} twitter handles")

    def get_high_rep_handles(self):
        accounts = []
        results = self.cyphers.get_high_rep_handles()
        if DEBUG:
            results = results[:10]
        for entry in results:
            accounts.append(
                {"userId": entry.get("t.userId"), "handle": entry.get("t.handle"), "rep": entry.get("reputation")}
            )

        logging.info(f"Found {len(accounts)} twitter handles")
        return accounts

    def process_followers(self):
        accounts = self.get_high_rep_handles()
        logging.info("Getting followers")
        chunk_id = 0
        for i in tqdm(range(0, len(accounts), self.chunk_size)):
            followers = []
            following = []
            chunk = accounts[i: i+self.chunk_size]
            for account in tqdm(chunk, desc="Getting followers"):
                userId = account.get("userId")
                lastChecked = self.metadata["followers"].get(userId, self.runtime)
                if lastChecked < self.cutoff:
                    continue
                
                followers.extend(self.get_followers(account))
                following.extend(self.get_following(account))

                self.metadata["followers"][userId] = self.runtime.timestamp()
            
            self.data["followers"] = followers
            self.data["following"] = following
            self.save_data(chunk_prefix=chunk_id)
            self.save_metadata()
            chunk_id += 1

    def get_followers(self, account):
        follower_url = "https://api.twitter.com/2/users/{}/followers?max_results=1000{}&user.fields=username"
        results = []
        items = self.twitter_api_call(follower_url, account["userId"], results=[])
        for follower in items:
            results.append(
                {
                    "handle": account.get("handle").lower(),
                    "follower": follower.get("username").lower(),
                }
            )
        return results

    def get_following(self, account):
        following_url = "https://api.twitter.com/2/users/{}/following?max_results=1000{}&user.fields=username"
        results = []
        items = self.twitter_api_call(following_url, account["userId"], results=[])
        for follower in items:
            results.append(
                {
                    "handle": account.get("handle").lower(),
                    "follower": follower.get("username").lower(),
                }
            )
        return results

    def ingest_followers(self):
        logging.info("Loading the data from S3")
        self.load_data()
        logging.info("Ingesting data")
        handles = set()
        for entry in self.data["followers"] + self.data["following"]:
            handles.add(entry.get("handle"))
            handles.add(entry.get("follower"))
        handles = [{"handle": handle, "profileUrl": f"https://twitter.com/{handle}"} for handle in handles]

        handle_urls = self.save_json_as_csv(handles, self.bucket_name, f"processor_handles_{self.asOf}")
        self.cyphers.create_or_merge_twitter_nodes(handle_urls)

        follower_urls = self.save_json_as_csv(self.data["followers"], self.bucket_name, f"processor_followers_{self.asOf}")
        self.cyphers.merge_followers_relationships(follower_urls)

        following_urls = self.save_json_as_csv(self.data["following"], self.bucket_name, f"processor_following_{self.asOf}")
        self.cyphers.merge_following_relationships(following_urls)

        self.metadata["last_date_ingested"] = f"{self.runtime.year}-{self.runtime.month}-{self.runtime.day}"

    def run(self):
        self.process_followers()
        self.ingest_followers()

if __name__ == "__main__":
    processor = TwitterFollowersPostProcess()
    processor.run()
