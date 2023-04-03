from ..helpers import Ingestor
from .cyphers import ENSCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging


class ENSIngestor(Ingestor):
    def __init__(self):
        self.cyphers = ENSCyphers()
        super().__init__("ens-records")

    def prepare_domains(self):
        domains = pd.DataFrame(self.scraper_data["domains"])
        domains = domains[domains["name"].apply(len) < 400]
        domains = domains[~domains["name"].isna()]
        domains = domains[~domains["createdAt"].isna()]
        resolvedAddresses = domains[~domains["resolvedAddress"].isna()]
        owners = domains[~domains["owner"].isna()]
        wallets = set(list(resolvedAddresses["resolvedAddress"].unique()) + list(resolvedAddresses["owner"].unique()))
        wallets = [{"address": address} for address in wallets if not self.is_zero_address(address)]
        return domains, resolvedAddresses, owners, wallets
    
    def ingest_domains(self):
        logging.info("Ingesting Domains")
        domains, resolvedAddresses, owners, wallets = self.prepare_domains()

        urls = self.save_json_as_csv(wallets, f"ingest_domains_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.save_df_as_csv(domains, f"ingest_domains_{self.asOf}")
        self.cyphers.create_or_merge_ens_domains(urls)

        urls = self.save_df_as_csv(resolvedAddresses, f"ingest_resolvedAddresses_{self.asOf}")
        self.cyphers.link_ENS_resolved_addresses(urls)

        urls = self.save_df_as_csv(owners, f"ingest_owners_{self.asOf}")
        self.cyphers.link_ENS_owners(urls)
    
    def prepare_registrations(self):
        registrations = pd.DataFrame(self.scraper_data["registrations"])
        registrations = registrations[~registrations["name"].isna()]
        registrations = registrations[~registrations["transactionID"].isna()]
        registrations = registrations[~registrations["owner"].isna()]
        wallets = [{"address": address} for address in registrations["owner"].unique() if not self.is_zero_address(address)]
        return registrations, wallets

    def ingest_registrations(self):
        logging.info("Ingesting Registrations")
        registrations, wallets = self.prepare_registrations()

        urls = self.save_json_as_csv(wallets, f"ingest_registrations_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.save_df_as_csv(registrations, f"ingest_registrations_{self.asOf}")
        self.cyphers.link_ENS_registrations(urls)

    def prepare_transfers(self):
        transfers = pd.DataFrame(self.scraper_data["transfers"])
        transfers = transfers[~transfers["name"].isna()]
        transfers = transfers[~transfers["from"].isna()]
        transfers = transfers[~transfers["transactionID"].isna()]
        transfers = transfers[~transfers["to"].isna()]
        transfers["isZeroAddress"] = transfers["to"].apply(self.is_zero_address)
        wallets = set(list(transfers["from"].unique()) + list(transfers["to"].unique()))
        wallets = [{"address": address} for address in wallets if not self.is_zero_address(address)]
        burns = transfers[transfers["isZeroAddress"]]
        transfers = transfers[~transfers["isZeroAddress"]]
        return transfers, burns, wallets
    
    def ingest_transfers(self):
        logging.info("Ingesting Transfers")
        transfers, burns, wallets = self.prepare_transfers()

        urls = self.save_json_as_csv(wallets, f"ingest_transfers_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        urls = self.save_df_as_csv(burns, f"ingest_burns_{self.asOf}")
        self.cyphers.link_ENS_burns(urls)

        urls = self.save_df_as_csv(transfers, f"ingest_transfers_{self.asOf}")
        self.cyphers.link_ENS_transfers(urls)

    def run(self):
        self.ingest_domains()
        self.ingest_registrations()
        self.ingest_transfers()

if __name__ == "__main__":
    ingestor = ENSIngestor()
    ingestor.run()
