from ..helpers import Ingestor
from .cyphers import FollowerCyphers
import datetime
import pandas as pd
import logging


class FollowerIngestor(Ingestor):
    def __init__(self):
        self.cyphers = FollowerCyphers()
        super().__init__("twitter")

    def ingest_followers(self):
        data = self.s3.load_csv(self.bucket_name, "twitter_followers.csv")
        new_handles = [{"handle": x} for x in data["follower"].unique()]
        urls = self.s3.save_json_as_csv(new_handles, self.bucket_name, f"ingestor_twitter_follower_handles_{self.asOf}")
        self.cyphers.create_twitter(urls)

        urls = self.s3.save_df_as_csv(data, self.bucket_name, f"ingestor_twitter_followers_{self.asOf}")
        self.cyphers.link_twitter_followers(urls)

    def ingest_following(self):
        data = self.s3.load_csv(self.bucket_name, "twitter_following.csv")
        new_handles = [{"handle": x} for x in data["handle"].unique()]
        urls = self.s3.save_json_as_csv(
            new_handles, self.bucket_name, f"ingestor_twitter_following_handles_{self.asOf}"
        )
        self.cyphers.create_twitter(urls)

        urls = self.s3.save_df_as_csv(data, self.bucket_name, f"ingestor_twitter_following_handles_{self.asOf}")
        self.cyphers.link_twitter_followers(urls)

    def run(self):
        self.ingest_followers()
        self.ingest_following()


if __name__ == "__main__":
    ingestor = FollowerIngestor()
    ingestor.run()
