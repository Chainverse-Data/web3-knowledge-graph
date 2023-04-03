from datetime import datetime, timezone
import logging
import os
import time

import numpy as np
from .requests import Requests

DEBUG = os.environ.get("DEBUG", False)

class Twitter(Requests):
    def __init__(self) -> None:
        super().__init__()
        self.api_url = "https://api.twitter.com/2"
        self.tweets_api_url = self.api_url + "/tweets"
        self.twitter_api_tokens = [el.strip() for el in os.environ.get("TWITTER_BEARER_TOKEN", "").split(",")]
        self.rate_limited = {}
        for token in self.twitter_api_tokens:
            self.rate_limited[token] = False

    def check_rate_limit(self, response, token):
        try:
            content = response.json()
        except:
            return False
        if content["title"] == "Too Many Requests":
            self.rate_limited[token] = True
            time_to_wait = int(response.headers["x-rate-limit-reset"]) - int(datetime.now(timezone.utc).timestamp())
            logging.warning(f"Rate limit exceeded. Waiting {time_to_wait} seconds.")
            time.sleep(time_to_wait)
            self.rate_limited[token] = False

    def get_headers(self):
        token = np.random.choice(self.twitter_api_tokens)
        while self.rate_limited[token]:
            token = np.random.choice(self.twitter_api_tokens)
        twitter_headers = {
            "Authorization": f"Bearer {token}",
        }
        return twitter_headers

    def get_request(self, url, params=None, counter=0):
        if counter > 10:
            return None
        response = super().get_request(url, params=params, headers=self.get_headers(), json=False, decode=False, ignore_retries=True)
        if not self.check_rate_limit(response) or response.status_code != 200:
            return self.get_request(url, params=params, counter=counter+1)
        return response.json()

    def search_tweet(self, query, user_info=False, since_id=None, tweets=[], users=[], meta={"newest_id": 0, "oldest_id": np.inf}, max_results=100, next_token=None):
        if DEBUG and len(tweets) > 500:
            return tweets, users, meta
        params = {
            "query": query,
            "tweet.fields": "text,author_id,created_at,id,conversation_id",
            "max_results": max_results
        }
        if user_info: params["expansions"] = "author_id"
        if user_info: params["user.fields"] = "name,username"
        if since_id: params["since_id"] = since_id
        if next_token: params["next_token"] = next_token
        url = self.tweets_api_url + "/search/recent"
        result = self.get_request(url, params=params)
        if result and type(result) == dict:
            if "data" in result: 
                tweets.extend(result["data"])
            if "includes" in result and "users" in result["includes"]:
                users.extend(result["includes"]["users"])
            if "meta" in result:
                if "newest_id" in result["meta"]: meta["newest_id"] = max(meta["newest_id"], int(result["meta"]["newest_id"]))
                if "oldest_id" in result["meta"]: meta["oldest_id"] = min(meta["oldest_id"], int(result["meta"]["oldest_id"]))
                if "next_token" in result["meta"]:
                    return self.search_tweet(query, user_info=user_info, since_id=since_id, tweets=tweets, users=users, meta=meta, max_results=max_results, next_token=result["meta"]["next_token"])
        return tweets, users, meta

    def get_tweet_conversation(self, conversation_id, user_info=False, since_id=None, max_results=100):
        query = f"conversation_id:{conversation_id}"
        tweets, users, meta = self.search_tweet(query, user_info=user_info, since_id=since_id, max_results=max_results)
        return tweets, users, meta


    
