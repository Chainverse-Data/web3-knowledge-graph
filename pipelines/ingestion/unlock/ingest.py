from ..helpers import Ingestor
from .cyphers import UnlockCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class UnlockIngestor(Ingestor):
    def __init__(self):
        self.cyphers = UnlockCyphers()
        super().__init__("unlock")

    def prepare_unlock_data(self):
        locks = pd.DataFrame(self.scraper_data["locks"])
        keys = pd.DataFrame(self.scraper_data["keys"])
        managers = pd.DataFrame(self.scraper_data["managers"])
        holders = pd.DataFrame(self.scraper_data["holders"])

        logging.info(
            "Ingested data... there are {lockRows} and {keyRows}".format(lockRows=len(locksDf), keyRows=len(keysDf))
        )

        holders["tokenId"] = holders["keyId"].apply(lambda x: x.split("-")[1])

        ### wallets
        manager_wallets = list(managers["address"])
        holder_wallets = list(holders["address"])
        wallets = set(manager_wallets + holder_wallets)
        wallets = pd.DataFrame({"address": list(wallets)})

        ## tokens
        tokens = set(list(locks["address"]))
        tokens = pd.DataFrame({"address": list(tokens)})

        return {
            "locks": locks,
            "keys": keys,
            "managers": managers,
            "holders": holders,
            "wallets": wallets,
            "tokens": tokens,
        }

    def ingest_wallets(self, data=None):
        urls = self.s3.save_df_as_csv(
            data["wallets"], self.bucket_name, f"ingestor_wallets_{self.asOf}", ACL="public-read"
        )
        self.cyphers.create_unlock_wallets(urls)

    def ingest_lock_metadata(self, data=None):
        urls = self.s3.save_df_as_csv(data["locks"], self.bucket_name, f"ingestor_locks_{self.asOf}", ACL="public-read")
        self.cyphers.create_locks(urls)

    def ingest_key_metadata(self, data=None):
        urls = self.s3.save_df_as_csv(data["keys"], self.bucket_name, f"ingestor_keys_{self.asOf}", ACL="public-read")
        self.cyphers.create_keys(urls)

    def ingest_lock_managers(self, data=None):
        urls = self.s3.save_df_as_csv(
            data["managers"], self.bucket_name, f"ingestor_lock_managers_{self.asOf}", ACL="public-read"
        )
        self.cyphers.create_lock_managers(urls)

    def ingest_key_holders(self, data=None):
        urls = self.s3.save_df_as_csv(
            data["holders"], self.bucket_name, f"ingestor_key_holders_{self.asOf}", ACL="public-read"
        )
        self.cyphers.create_key_holders(urls)

    def ingest_locks_keys(self, data=None):
        logging.info("Connecting locks and keys...")
        locks = data["allTokensUnique"]
        urls = self.s3.save_df_as_csv(locks, self.bucket_name, f"ingest_locks_keys{self.asOf}", ACL="public-read")
        self.cyphers.connect_locks_keys(urls)
        logging.info("successfully connected locks to keys. good job dev!")

    def run(self):
        unlock_data = self.prepare_unlock_data()
        self.ingest_wallets(data=unlock_data)
        self.ingest_lock_metadata(data=unlock_data)
        self.ingest_key_metadata(data=unlock_data)
        self.ingest_lock_managers(data=unlock_data)
        self.ingest_key_holders(data=unlock_data)
        self.ingest_locks_keys(data=unlock_data)


if __name__ == "__main__":
    ingestor = UnlockIngestor()
    ingestor.run()
