from ..helpers import Ingestor
from .cyphers import DelegationCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging

class DelegationIngestor(Ingestor):
    def __init__(self):
        self.cyphers = DelegationCyphers()
        super().__init__('gab')
    
    def prepare_delegation_data(self):
        logging.info("Preparing data...")
        delegateChangesDf = pd.DataFrame(self.scraper_data['delegateChanges'])
        delegateTokens = delegateChangesDf[["protocol", "tokenAddress"]].drop_duplicates()
        delegateTokens.rename(columns={"tokenAddress": "contractAddress"})
        delegateTokens["symbol"] = delegateTokens["protocol"]
        delegateTokens["decimal"] = "-1"
        logging.info(f"Preparing delegate changes DF. Done!")
        delegateVotingPowerChanges = pd.DataFrame(self.scraper_data['delegateVotingPowerChanges'][1:])
        logging.info("Preparing delegate voting power changes. Done!")
        delegatesDf = pd.DataFrame(self.scraper_data['delegates'])
        logging.info("Preparing delegates. Done!")
        tokenHoldersDf = pd.DataFrame(self.scraper_data['tokenHolders'])
        ## make wallet list
        delegateWallets = list(delegateChangesDf['delegate'])
        delegatorWallets = list(delegateChangesDf['delegator'])
        tokenHolderWallets = list(tokenHoldersDf['id'])
        allWallets = delegateWallets + delegatorWallets + tokenHolderWallets
        allWalletsDf = pd.DataFrame()
        allWalletsDf['address'] = allWallets
        logging.info("Preparing tokenHolders. Done!")
        return {
            'delegateChangesDf': delegateChangesDf,
            'tokenStrategies': delegateTokens,
            'allWalletsDf': allWalletsDf,
            'delegateVotingPowerChanges': delegateVotingPowerChanges
        }

    def ingest_wallets(self, data=None):
        logging.info("Ingesting wallets...")
        urls = self.s3.save_df_as_csv(data['allWalletsDf'], self.bucket_name, f"ingestor_wallets_{self.asOf}", ACL='public-read')
        self.cyphers.create_or_merge_wallets(urls)
        logging.info(f"successfully ingested wallets. good job dev!")
    
    def ingest_delegate_changes(self, data=None):
        logging.info("Ingesting delegate changes...")
        urls = self.s3.save_df_as_csv(data['delegateChangesDf'], self.bucket_name, f"ingestor_delegate_changes_{self.asOf}", ACL='public-read')
        self.cyphers.create_delegation_events(urls)
        logging.info(f"successfully ingested delegate changes. good job dev!")
    
    def ingest_delegate_connections(self, data=None):
        logging.info("Ingesting delegate connections...")
        urls = self.s3.save_df_as_csv(data['delegateChangesDf'], self.bucket_name, f"ingestor_delegate_connections_{self.asOf}", ACL='public-read')
        self.cyphers.connect_delegate_events(urls)
        logging.info(f"successfully ingested delegate connections. good job dev!")

    def ingest_token_strategies(self, data=None):
        logging.info("Ingesting strategies connections...")
        urls = self.s3.save_df_as_csv(data['tokenStrategies'], self.bucket_name, f"ingestor_delegate_strategies_{self.asOf}", ACL='public-read')
        self.cyphers.create_or_merge_tokens(urls)
        self.cyphers.create_or_merge_entities(urls)
        self.cyphers.connect_strategies(urls)
        logging.info(f"successfully ingested strategies. good job dev!")

    def enrich_delegate_events(self, data=None):
        logging.info("Enriching delegate events...")
        delegateVotingPowerChanges = data['delegateVotingPowerChanges']
        urls = self.s3.save_df_as_csv(delegateVotingPowerChanges, self.bucket_name, f"ingestor_delegate_voting_power_changes_{self.asOf}", ACL='public-read')
        self.cyphers.enrich_delegation_events(urls)
        logging.info(f"successfully enriched delegate events. good job dev!")
    
    def run(self):
        delegationData = self.prepare_delegation_data()
        self.ingest_wallets(data=delegationData)
        self.ingest_delegate_changes(data=delegationData)
        self.ingest_delegate_connections(data=delegationData)
        self.enrich_delegate_events(data=delegationData)
        self.save_metadata()

if __name__== '__main__':
    ingestor = DelegationIngestor()
    ingestor.run()






        
