from datetime import datetime
import logging

import pandas as pd
from ...helpers import Twitter
from ..helpers import Processor
from .cyphers import TwitterThreadsCyphers
import os
import re

DEBUG = os.environ.get("DEBUG", False)

class TwitterThreadsProcessor(Processor):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""

    def __init__(self):
        self.cyphers = TwitterThreadsCyphers()
        self.twitter = Twitter()
        super().__init__("twitter-threads")
        self.last_tweet_id = self.metadata.get("last_tweet_id", None)
        self.address_matcher = re.compile("(0x[a-zA-Z0-9]{40})")
        self.ens_matcher = re.compile("([-a-zA-Z0-9@:%._\+~#=]{1,256}\.eth)")
        self.current_latest_id = 0
        self.search_queries = [
            "Drop ENS -is:retweet",
            'drop wallet (eth OR BNB) -is:retweet',
            ]
        self.data["users"] = {}
        self.data["matches"] = {}
        self.data["threads"] = {}

    def find_valid_address(self, tweet) -> list:
        if "text" in tweet:
            return self.address_matcher.findall(tweet["text"])
        return []

    def find_valid_ens(self, tweet) -> list:
        if "text" in tweet:
            return self.ens_matcher.findall(tweet["text"])
        return []

    def extract_address_and_ens(self, tweets) -> None:
        for tweet in tweets:
            author_id = tweet["author_id"]
            if author_id not in self.data["matches"]:
                self.data["matches"][author_id] = {
                    "addresses": set(),
                    "ens": set(),
                    "tweets": set()
                }
            for address in self.find_valid_address(tweet):
                self.data["matches"][author_id]["addresses"].add(address)
                self.data["matches"][author_id]["tweets"].add(tweet["id"])
            for address in self.find_valid_ens(tweet):
                self.data["matches"][author_id]["ens"].add(address)
                self.data["matches"][author_id]["tweets"].add(tweet["id"])

    def extract_usernames(self, users) -> None:
        for user in users:
            if user["id"] not in self.data["matches"]:
                self.data["users"][user["id"]] = {
                    "username": user["username"],
                    "name": user["name"],
                }
        
    def update_current_tweet_id(self, meta):
        if "newest_id" in meta:
            self.current_latest_id = max(meta["newest_id"], self.current_latest_id)

    def update_threads(self, tweets):
        for tweet in tweets:
            conversation_id = tweet["conversation_id"]
            tweet_created_at = datetime.strptime(tweet["created_at"] ,"%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
            if conversation_id not in self.data["threads"]:
                self.data["threads"][conversation_id] = {"last_update": 0, "replies": [], "author_id": None}
            self.data["threads"][conversation_id]["last_update"] = max(self.data["threads"][conversation_id]["last_update"], tweet_created_at)
            author_id = tweet["author_id"]
            self.data["threads"][conversation_id]["replies"].append({"author_id": author_id, "created_at": tweet_created_at})

    def parse_results(self, tweets, users, meta):
        self.extract_usernames(users)
        self.extract_address_and_ens(tweets)
        self.update_threads(tweets)
        self.update_current_tweet_id(meta)

    def get_current_threads(self) -> list:
        conversations = self.cyphers.get_current_twitter_threads()
        return conversations

    def find_threads(self):
        logging.info("Searching for new conversations")
        conversations = set()
        for query in self.search_queries:
            logging.info(f"Now searching for: |{query}|")
            tweets, users, meta = self.twitter.search_tweet(query, user_info=True, since_id=self.last_tweet_id)
            self.parse_results(tweets, users, meta)
            for tweet in tweets:
                author_id = tweet["author_id"]
                self.data["threads"][tweet["conversation_id"]]["author_id"] = author_id
            for tweet in tweets:
                if "conversation_id" in tweet:
                    conversations.add(tweet["conversation_id"])
        logging.info(f"Found {len(conversations)} conversations")
        return conversations

    def get_conversation(self, conversation_id) -> None:
        tweets, users, meta = self.twitter.get_tweet_conversation(conversation_id, user_info=True, since_id=self.last_tweet_id)
        return (tweets, users, meta)

    def ingest_data(self):
        addresses = []
        ens = []
        threads = []
        replies = []
        for user_id in self.data["matches"]:
            if user_id in self.data["users"]:
                handle = self.data["users"][user_id]["username"]
                tweets = ",".join(list(self.data["matches"][user_id]["tweets"]))
                for address in self.data["matches"][user_id]["addresses"]:
                    addresses.append({"handle": handle, "address": address, "profileUrl": f"https://twitter.com/{handle}", "tweets": tweets})
                for name in self.data["matches"][user_id]["ens"]:
                    ens.append({"handle": handle, "name": name, "profileUrl": f"https://twitter.com/{handle}", "tweets": tweets})
        for thread in self.data["threads"]:
            author_id = self.data["threads"][thread]["author_id"]
            handle = ""
            if author_id:
                handle = self.data["users"][author_id]["username"]
            threads.append({"conversation_id": thread, "created_at": self.data["threads"][thread]["last_update"], "handle": handle})
            for reply in self.data["threads"][thread]["replies"]:
                handle = self.data["users"][reply["author_id"]]["username"]
                replies.append({"handle": handle, "created_at": reply["created_at"], "conversation_id": thread})
        replies = pd.DataFrame.from_dict(replies).drop_duplicates()

        addresses_urls = self.save_json_as_csv(addresses, self.bucket_name, f"process_addresses_{self.asOf}")
        ens_urls = self.save_json_as_csv(ens, self.bucket_name, f"process_ens_{self.asOf}")
        threads_urls = self.save_json_as_csv(threads, self.bucket_name, f"process_threads_{self.asOf}")
        replies_urls = self.save_df_as_csv(replies, self.bucket_name, f"process_replies_{self.asOf}")

        self.cyphers.create_or_merge_threads(threads_urls)
        self.cyphers.link_or_merge_thread_authors(threads_urls)

        self.cyphers.queries.create_wallets(addresses_urls)
        self.cyphers.queries.create_or_merge_twitter(addresses_urls)
        self.cyphers.link_or_merge_twitters_address(addresses_urls)
        
        self.cyphers.queries.create_or_merge_ens_alias(ens_urls)
        self.cyphers.queries.create_or_merge_twitter(ens_urls)
        self.cyphers.link_or_merge_twitters_ens(ens_urls)

        self.cyphers.link_or_merge_thread_replies(replies_urls)
        
    def run(self):
        conversations = self.get_current_threads()
        conversations += self.find_threads()
        conversations = list(set(conversations))
        if DEBUG:
            conversations = conversations[:10]
        data = self.parallel_process(self.get_conversation, conversations, description="Getting latest tweets from conversations")
        for tweets, users, meta in data:
            self.parse_results(tweets, users, meta)
        self.ingest_data()
        self.metadata["last_tweet_id"] = self.current_latest_id
        self.save_metadata()
        
if __name__ == "__main__":
    processor = TwitterThreadsProcessor()
    processor.run()
