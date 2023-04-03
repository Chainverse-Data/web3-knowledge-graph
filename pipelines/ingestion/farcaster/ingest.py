from ..helpers import Ingestor
from .cyphers import FarcasterCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class FarcasterIngestor(Ingestor):
    def __init__(self):
        self.cyphers = FarcasterCyphers()
        super().__init__("farcaster")

    def ingest_users(self):
        users = pd.DataFrame(self.scraper_data["users"])

        wallets = set(users["address"])
        wallets = [{"address": wallet} for wallet in wallets]
        urls = self.save_json_as_csv(wallets, f"ingestor_farcaster_wallets_{self.asOf}")
        self.cyphers.create_farcaster_wallets(urls)

        urls = self.save_df_as_csv(users, f"ingestor_users_{self.asOf}")
        self.cyphers.create_or_merge_farcaster_users(urls)
        self.cyphers.link_users_wallets(urls)

    def run(self):
        self.ingest_users()


if __name__ == "__main__":
    ingestor = FarcasterIngestor()
    ingestor.run()
