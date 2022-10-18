from ..helpers import Ingestor
from .cyphers import MultisigCyphers
import datetime
import pandas
from typing import Dict, List, Any


class MultisigIngestor(Ingestor):
    def __init__(self):
        self.cyphers = MultisigCyphers()
        super().__init__("multisig")
        self.metadata["last_date_ingested"] = self.end_date
        if isinstance(self.start_date, datetime.datetime):
            self.metadata["last_date_ingested"] = self.end_date.strftime("%Y-%m-%d")

    def ingest_multisig(self):
        print("Ingesting multisig...")

        all_wallets = set(
            [x["multisig"] for x in self.scraper_data["multisig"]]
            + [x["address"] for x in self.scraper_data["multisig"]]
        )
        wallet_dict = [{"address": wallet} for wallet in all_wallets if wallet != "" and wallet is not None]

        # add multisig and owner wallet nodes
        urls = self.s3.save_json_as_csv(wallet_dict, self.bucket_name, f"ingestor_wallets_{self.asOf}")
        self.cyphers.create_or_merge_multisig_wallets(urls)

        multisig_dict = (
            pandas.DataFrame(self.scraper_data["multisig"]).drop_duplicates(subset=["multisig"]).to_dict("records")
        )

        # add multisig labels
        urls = self.s3.save_json_as_csv(multisig_dict, self.bucket_name, f"ingestor_multisig_{self.asOf}")
        self.cyphers.add_multisig_labels(urls)

        multisig_dict = (
            pandas.DataFrame(self.scraper_data["multisig"])
            .drop_duplicates(subset=["multisig", "address"])
            .to_dict("records")
        )

        # add signer relationships (multisig-wallet)
        urls = self.s3.save_json_as_csv(multisig_dict, self.bucket_name, f"ingestor_multisig_{self.asOf}")
        self.cyphers.link_multisig_signer(urls)

    def run(self):
        self.ingest_multisig()
        self.save_metadata()


if __name__ == "__main__":
    ingestor = MultisigIngestor()
    ingestor.run()
