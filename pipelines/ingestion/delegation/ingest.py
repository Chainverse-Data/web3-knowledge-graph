from ..helpers import Ingestor
from .cyphers import DelegationCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging

class DelegationIngestor(Ingestor):
    def __init__(self):
        self.cyphers = DelegationCyphers()
        super().__init__('delegation')
    
    def prepare_delegation_data(self):
        logging.info("Preparing data...")

        data = {
            "wallets": [],
            "entities": [],
            "tokens": [],
            "delegationsNew": [],
            "delegationsRemoval": [],
            "delegates": []
        }

        uniqueWallets = set()
        uniqueProtocols = set()
        for change in self.scraper_data['delegateChanges']:
            uniqueWallets.add(change["delegator"])
            uniqueWallets.add(change["delegate"])
            uniqueProtocols.add(change["protocol"])
            
            data["tokens"].append(
                {
                    "protocol": change["protocol"], 
                    "contractAddress": change["tokenAddress"],
                    "symbol": "",
                    "decimal": -1
                })
            data["delegationsNew"].append(
                {
                    "protocol": change["protocol"], 
                    "delegate": change["delegate"],
                    "delegator": change["delegator"],
                    "txHash": change["txnHash"]
                })
            data["delegationsRemoval"].append(
                {
                    "protocol": change["protocol"], 
                    "delegate": change["previousDelegate"],
                    "delegator": change["delegator"],
                })


        for holder in self.scraper_data['tokenHolders']:
            uniqueWallets.add(holder["id"])
        
        data["wallets"] = [{"address": address} for address in uniqueWallets]
        data["entities"] = [{"protocol": protocol} for protocol in uniqueProtocols]

        delegates = {}
        for delegate in self.scraper_data['delegates']:
            if delegate["protocol"] not in delegates:
                delegates[delegate["protocol"]] = {}
            delegates[delegate["protocol"]][delegate["id"]] = {
                "delegatedVotesRaw": delegate["delegatedVotesRaw"],
                "delegatedVotes": delegate["delegatedVotes"],
                "numberVotes": delegate["numberVotes"],
            }
        for change in self.scraper_data['delegateVotingPowerChanges']:
            if change["delegate"] not in delegates[change["protocol"]]:
                delegates[change["protocol"]][change["delegate"]] = {
                    "delegatedVotesRaw": 0,
                    "delegatedVotes": 0,
                    "numberVotes": 0,
                }
            delegates[change["protocol"]][change["delegate"]]["previousBalance"] = change["previousBalance"]
            delegates[change["protocol"]][change["delegate"]]["newBalance"] = change["newBalance"]
        
        delegatesData = []
        for protocol in delegates:
            for delegate in delegates[protocol]:
                delegates[protocol][delegate]["protocol"] = protocol
                delegates[protocol][delegate]["delegate"] = delegate
                delegatesData.append(delegates[protocol][delegate])

        data["delegates"] = delegatesData

        data["wallets"] = pd.DataFrame.from_dict(data["wallets"]).drop_duplicates()
        data["entities"] = pd.DataFrame.from_dict(data["entities"]).drop_duplicates()
        data["tokens"] = pd.DataFrame.from_dict(data["tokens"]).drop_duplicates()
        data["delegationsNew"] = pd.DataFrame.from_dict(data["delegationsNew"]).drop_duplicates()
        data["delegationsRemoval"] = pd.DataFrame.from_dict(data["delegationsRemoval"]).drop_duplicates()
        data["delegates"] = pd.DataFrame.from_dict(data["delegates"]).drop_duplicates()
        
        return data

    def ingest_wallets_entities_tokens(self, data):
        logging.info(f"""Ingesting {len(data["wallets"])} wallets...""")
        urls = self.save_df_as_csv(data["wallets"], f"ingestor_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)

        logging.info(f"""Ingesting {len(data["entities"])} Entities and delegations...""")
        urls = self.save_df_as_csv(data["entities"], f"ingestor_entities_{self.asOf}")
        self.cyphers.create_or_merge_entities(urls)
        self.cyphers.create_or_merge_delegations(urls)
        self.cyphers.link_or_merge_delegation_to_entity(urls)

        logging.info(f"""Ingesting {len(data["tokens"])} Tokens...""")
        urls = self.save_df_as_csv(data["tokens"], f"ingestor_tokens_{self.asOf}", max_lines=1000)
        self.cyphers.create_or_merge_tokens(urls)
        self.cyphers.link_or_merge_strategies(urls)
        self.cyphers.link_or_merge_delegation_to_token(urls)

    def ingest_delegations(self, data):
        logging.info(f"""Deleting {len(data["delegationsRemoval"])} removed delegations...""")
        urls = self.save_df_as_csv(data["delegationsRemoval"], f"ingestor_delegationRemoval_{self.asOf}", max_lines=2000)
        self.cyphers.detach_wallets_delegations(urls)
        self.cyphers.detach_wallets_delegators(urls)
        self.cyphers.detach_wallets_delegates(urls)

        logging.info(f"""Adding {len(data["delegationsNew"])} new_delegations...""")
        urls = self.save_df_as_csv(data["delegationsNew"], f"ingestor_delegationNew_{self.asOf}", max_lines=2000)
        self.cyphers.link_or_merge_wallet_delegators(urls)
        self.cyphers.link_or_merge_wallets_delegations(urls)
        
        logging.info(f"""Linking {len(data["delegates"])} Delegating wallets...""")
        urls = self.save_df_as_csv(data["delegates"], f"ingestor_delegations_{self.asOf}", max_lines=2000)
        self.cyphers.link_or_merge_wallet_delegates(urls)

    def run(self):
        delegationData = self.prepare_delegation_data()
        self.ingest_wallets_entities_tokens(delegationData)
        self.ingest_delegations(delegationData)
        self.save_metadata()

if __name__== '__main__':
    ingestor = DelegationIngestor()
    ingestor.run()






        
