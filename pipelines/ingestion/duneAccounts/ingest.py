from ..helpers import Ingestor
from .cyphers import DuneCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging
from tqdm import tqdm


class DuneIngestor(Ingestor):
    def __init__(self):
        self.cyphers = DuneCyphers()
        super().__init__("dune-accounts")

    def clean_data(self):
        nodes = {"twitter": set(), "telegram": set(), "discord": set(), "address": set(), "users": [], "teams": []}
        rels = {"address": [], "account": [], "teams": []}

        data = self.scraper_data["users"] + self.scraper_data["teams"]

        for _, row in tqdm(enumerate(data)):
            if row.get("Twitter", None):
                x = row["Twitter"].split("/")[-1].replace("@", "")
                if x != "":
                    nodes["twitter"].add(x.lower())
                    rels["account"].append({"handle": x.lower(), "url": row["url"], "name": row["name"]})
            if row.get("Telegram", None):
                x = row["Telegram"].split("/")[-1].replace("@", "")
                if x != "":
                    nodes["telegram"].add(x.lower())
                    rels["account"].append({"handle": x.lower(), "url": row["url"], "name": row["name"]})
            if row.get("Discord", None) and "#" in row["Discord"]:
                nodes["discord"].add(row["Discord"].lower())
                rels["account"].append({"handle": row["Discord"].lower(), "url": row["url"], "name": row["name"]})
            if row.get("address", None):
                nodes["address"].add(row["address"].lower())
                rels["address"].append({"address": row["address"].lower(), "url": row["url"], "name": row["name"]})

        for entry in self.scraper_data["teams"]:
            entry["description"] = self.cyphers.sanitize_text(row.get("description", ""))
            nodes["teams"].append(entry)

        for entry in self.scraper_data["users"]:
            entry["description"] = self.cyphers.sanitize_text(row.get("description", ""))
            nodes["users"].append(entry)
            for team in entry.get("teams", []):
                rels["teams"].append({"url": row["url"], "team": team, "name": row["name"]})

        nodes["twitter"] = [{"handle": handle, "url": f"https://twitter.com/{handle}"} for handle in nodes["twitter"]]
        nodes["telegram"] = [{"handle": handle, "url": f"https://t.me/{handle}"} for handle in nodes["telegram"]]
        nodes["discord"] = [{"handle": handle} for handle in nodes["discord"]]
        nodes["address"] = [{"address": address} for address in nodes["address"]]

        return nodes, rels

    def handle_nodes(self, nodes):
        urls = self.save_json_as_csv(nodes["twitter"], self.bucket_name, "dune-twitter")
        self.cyphers.create_or_merge_dune_twitter(urls)

        urls = self.save_json_as_csv(nodes["telegram"], self.bucket_name, "dune-telegram")
        self.cyphers.create_or_merge_dune_telegram(urls)

        urls = self.save_json_as_csv(nodes["discord"], self.bucket_name, "dune-discord")
        self.cyphers.create_or_merge_dune_discord(urls)

        urls = self.save_json_as_csv(nodes["address"], self.bucket_name, "dune-address")
        self.cyphers.create_or_merge_dune_wallets(urls)

        urls = self.save_json_as_csv(nodes["users"], self.bucket_name, "dune-users")
        self.cyphers.create_or_merge_dune_users(urls)

        urls = self.save_json_as_csv(nodes["teams"], self.bucket_name, "dune-teams")
        self.cyphers.create_or_merge_dune_teams(urls)

    def handle_rels(self, rels):
        urls = self.save_json_as_csv(rels["address"], self.bucket_name, "dune-address-rels")
        self.cyphers.link_dune_wallets(urls)

        urls = self.save_json_as_csv(rels["account"], self.bucket_name, "dune-account-rels")
        self.cyphers.link_dune_accounts(urls)

        urls = self.save_json_as_csv(rels["teams"], self.bucket_name, "dune-teams-rels")
        self.cyphers.link_dune_teams(urls)

    def run(self):
        nodes, rels = self.clean_data()
        self.handle_nodes(nodes)
        self.handle_rels(rels)


if __name__ == "__main__":
    ingestor = DuneIngestor()
    ingestor.run()
