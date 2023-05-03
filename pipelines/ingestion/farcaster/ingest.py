from ..helpers import Ingestor
from .cyphers import FarcasterCyphers
import logging


class FarcasterIngestor(Ingestor):
    def __init__(self):
        self.cyphers = FarcasterCyphers()
        super().__init__("farcaster")

    def ingest_users(self):
        data = self.scraper_data["users"]
        wallets = set()
        users = []
        for user in data:
            wallets.add(user["address"].lower())
            tmp = {
                "fid": user["fid"],
                "fname": user["fname"].lower(),
                "address": user["address"].lower(),
                "url": user["url"] or "",
                "bio": user["bio"] or "",
                "name": user["name"] or "",
                "profileUrl": user["profileUrl"] or "",
            }
            users.append(tmp)

        wallets = [{"address": wallet} for wallet in wallets]
        urls = self.save_json_as_csv(wallets, f"ingestor_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.save_json_as_csv(users, f"ingestor_users_{self.asOf}")
        self.cyphers.create_or_merge_farcaster_users(urls)
        self.cyphers.link_users_wallets(urls)

    def ingest_followers(self):
        data = self.scraper_data["followers"]
        urls = self.save_json_as_csv(data, f"ingestor_followers_{self.asOf}")
        self.cyphers.link_followers(urls)

    def run(self):
        self.ingest_users()
        self.ingest_followers()


if __name__ == "__main__":
    ingestor = FarcasterIngestor()
    ingestor.run()
