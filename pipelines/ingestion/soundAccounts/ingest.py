from ..helpers import Ingestor
from .cyphers import SoundCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging
from tqdm import tqdm


class SoundIngestor(Ingestor):
    def __init__(self):
        self.cyphers = SoundCyphers()
        super().__init__("sound-accounts")

    def clean_data(self):
        nodes = {"twitter": set(), "address": set(), "users": []}
        rels = {"address": [], "account": []}

        data = self.scraper_data["artists"]
        print("data", self.scraper_data)
        for _, row in tqdm(enumerate(data)):
            if row.get("name", None):
                nodes["users"].append({"name": row["name"].lower(), "url": row["url"]})
            else:
                continue
            if row.get("twitter", None):
                x = row["twitter"].split("/")[-1].replace("@", "")
                if x != "":
                    nodes["twitter"].add(x.lower())
                    rels["account"].append({"handle": x.lower(), "url": row["url"], "name": row["name"]})
            if row.get("address", None):
                nodes["address"].add(row["address"].lower())
                rels["address"].append({"address": row["address"].lower(), "url": row["url"], "name": row["name"]})

        nodes["twitter"] = [{"handle": handle, "url": f"https://twitter.com/{handle}"} for handle in nodes["twitter"]]
        nodes["address"] = [{"address": address} for address in nodes["address"]]

        return nodes, rels

    def handle_nodes(self, nodes):
        urls = self.save_json_as_csv(nodes["twitter"], "sound-twitter")
        self.cyphers.create_or_merge_sound_twitter(urls)

        urls = self.save_json_as_csv(nodes["address"], "sound-address")
        self.cyphers.create_or_merge_sound_wallets(urls)

        urls = self.save_json_as_csv(nodes["users"], "sound-users")
        self.cyphers.create_or_merge_sound_users(urls)

    def handle_rels(self, rels):
        urls = self.save_json_as_csv(rels["address"], "sound-address-rels")
        self.cyphers.link_sound_wallets(urls)

        urls = self.save_json_as_csv(rels["account"], "sound-account-rels")
        self.cyphers.link_sound_twitter(urls)

    def run(self):
        nodes, rels = self.clean_data()
        self.handle_nodes(nodes)
        self.handle_rels(rels)


if __name__ == "__main__":
    ingestor = SoundIngestor()
    ingestor.run()
