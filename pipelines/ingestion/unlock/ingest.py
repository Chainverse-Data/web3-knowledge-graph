from ..helpers import Ingestor, utils
from .cyphers import UnlockCyphers
import datetime as dt
from typing import Dict, List, Any
import logging

#TODO: Make readme for scraper and ingestor

class UnlockIngestor(Ingestor):
    def __init__(self):
        self.cyphers = UnlockCyphers()
        super().__init__("unlock")

        # used for filtering out burn addresses
        self.nullAddress = "0x0000000000000000000000000000000000000000"

    def ingest_locks(self):
        "This function ingests the unlock data loaded in self.data"
        logging.info("Ingesting lock data...")
        locks_data = self.process_locks()

        urls = self.s3.save_json_as_csv(locks_data, self.bucket_name, f"ingestor_locks_{self.asOf}")
        self.cyphers.create_or_merge_locks(urls)
        self.cyphers.link_or_merge_locks_to_keys(urls)

    def process_locks(self):
        logging.info("Processing locks data...")
        locks_data = []
        
        for lock in self.scraper_data["locks"]:
            if lock["tokenAddress"] != self.nullAddress and utils.is_valid_address(lock["address"]):
                tmp = {
                    "address": lock["address"].lower(),
                    "name": lock["name"].lower(),
                    "contractAddress": lock["tokenAddress"].lower(),
                    "creationBlock": lock["creationBlock"],
                    "price": lock["price"],                               
                    "expirationDuration": lock["expirationDuration"],
                    "totalSupply": int(lock["totalSupply"]),
                    "network": lock["network"].lower()
                }

                locks_data.append(tmp)
        
        return locks_data

    def ingest_managers(self):
        "This function ingests the managers data loaded in self.data"
        logging.info("Ingesting managers data...")
        managers_data = self.process_managers()

        urls = self.s3.save_json_as_csv(managers_data, self.bucket_name, f"ingestor_managers_{self.asOf}")
        self.cyphers.create_unlock_managers_wallets(urls)
        self.cyphers.link_or_merge_managers_to_locks(urls)
    
    def process_managers(self):
        logging.info("Processing managers data...")
        managers_data = []
        
        for manager in self.scraper_data["managers"]:
            if manager["lock"] != self.nullAddress and utils.is_valid_address(manager["address"]): 
                tmp = {
                    "lock": manager["lock"].lower(),
                    "address": manager["address"].lower()
                }

                managers_data.append(tmp)
        
        return managers_data
    
    def ingest_keys(self):
        "This function ingests the keys data loaded in self.data"
        logging.info("Ingesting keys data...")
        keys_data = self.process_keys() 

        urls = self.s3.save_json_as_csv(keys_data, self.bucket_name, f"ingestor_keys_{self.asOf}")
        self.cyphers.create_or_merge_keys(urls)

    def process_keys(self):
        logging.info("Processing keys data...")
        keys_data = []
        
        for key in self.scraper_data["keys"]:
            if key["address"] != self.nullAddress and utils.is_valid_address(key["address"]):
                tmp = {
                    "id": key["id"].lower(),
                    "contractAddress": key["address"].lower(),
                    "expiration": dt.datetime.fromtimestamp(key["expiration"]),
                    "tokenUri": key["tokenURI"].lower(),
                    "createdAt": dt.datetime.fromtimestamp(key["createdAt"]),
                    "network": key["network"].lower(),
                }

                keys_data.append(tmp)

        return keys_data

    def ingest_holders(self):
        "This function ingests the holders loaded in self.data"
        logging.info("Ingesting holders data...")    

        holders_data = self.process_holders()

        urls = self.s3.save_json_as_csv(holders_data, self.bucket_name, f"ingestor_holders_{self.asOf}")
        self.cyphers.create_unlock_holders_wallets(urls)
        self.cyphers.link_or_merge_holders_to_locks(urls)
        self.cyphers.link_or_merge_holders_to_keys(urls)

    def process_holders(self):
        logging.info("Processing holders data...")
        holders_data = []

        for holder in self.scraper_data["holders"]:
            if holder["tokenAddress"] != self.nullAddress  and utils.is_valid_address(holder["address"]):
                tmp = {
                    "address": holder["address"].lower(),
                    "keyId": holder["keyId"].lower(),       
                    "contractAddress": holder["tokenAddress"].lower()
                }
                holders_data.append(tmp)

        return holders_data

    def run(self):
        self.ingest_locks()
        self.ingest_managers()
        self.ingest_keys()
        self.ingest_holders()
        self.save_metadata()

if __name__ == "__main__":
    ingestor = UnlockIngestor()
    ingestor.run()
