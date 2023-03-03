from ..helpers import Ingestor
from .cyphers import LensCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class LensIngestor(Ingestor):
    def __init__(self):
        self.cyphers = LensCyphers()
        super().__init__("lens")

    def ingest_profiles(self):
        profiles = pd.DataFrame(self.scraper_data["profiles"])

        wallets = set(profiles["owner"])
        wallets = [{"address": wallet} for wallet in wallets]
        urls = self.save_json_as_csv(wallets, self.bucket_name, f"ingestor_lens_wallets_{self.asOf}")
        self.cyphers.create_lens_wallets(urls)

        urls = self.save_df_as_csv(profiles, self.bucket_name, f"ingestor_lens_profiles_{self.asOf}")
        self.cyphers.create_profiles(urls)
        self.cyphers.link_profiles_wallets(urls)

    def run(self):
        self.ingest_profiles()


if __name__ == "__main__":
    ingestor = LensIngestor()
    ingestor.run()
