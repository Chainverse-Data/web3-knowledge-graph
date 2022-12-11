from ..helpers import Ingestor
from .cyphers import DelegationCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging

class DelegationIngestor(Ingestor):
    def __init__(self):
        self.cyphers = DelegationCyphers()
        super().__init__('gitcoin-delegation-test')
    
    def prepare_delegation_data(self):
        logging.info("Preparing data...")
        delegateChangesDf = pd.DataFrame(self.scraper_data['delegateChanges'][1:])
        logging.info(f"Ingested delegate changes DF")
        delegateVotingPowerChanges = pd.DataFrame(self.scraper_data['delegateVotingPowerChanges'][1:])
        logging.info("Ingested delegate voting power changes")
        delegatesDf = pd.DataFrame(self.scraper_data['delegates'][1:])
        logging.info("Ingested delegates")
        tokenHoldersDf = pd.DataFrame(self.scraper_data['tokenHolders'][1:])
        ## make wallet list
        delegateWallets = list(delegateChangesDf['delegate'])
        delegatorWallets = list(delegateChangesDf['delegator'])
        tokenHolderWallets = list(tokenHoldersDf['tokenHolderAddress'])
        allWallets = delegateWallets + delegatorWallets + tokenHolderWallets
        allWalletsDf = pd.DataFrame()
        allWalletsDf['address'] = allWallets
        return {
            'delegateChangesDf': delegateChangesDf,
            'allWalletsDf': allWalletsDf,
            'delegateVotingPowerChanges': delegateVotingPowerChanges
        }

    def ingest_wallets(self, data=None):
        logging.info("Ingesting wallets...")
        allWalletsDf = data['allWalletsDf']
        urls = self.s3.save_df_as_csv(allWalletsDf, self.bucket_name, f"ingestor_wallets_{self.asOf}", ACL='public-read')
        self.cyphers.create_or_merge_wallets(urls)
        logging.info(f"successfully ingested wallets. good job dev!")
    
    def ingest_delegate_changes(self, data=None):
        logging.info("Ingesting delegate changes...")
        delegateChangesDf = data['delegateChangesDf']
        urls = self.s3.save_df_as_csv(delegateChangesDf, self.bucket_name, f"ingestor_delegate_changes_{self.asOf}", ACL='public-read')
        self.cyphers.create_delegation_events(urls)
        logging.info(f"successfully ingested delegate changes. good job dev!")
    
    def ingest_delegate_connections(self, data=None):
        logging.info("Ingesting delegate connections...")
        delegateConnectionsDf = data['delegateChangesDf']
        urls = self.s3.save_df_as_csv(delegateConnectionsDf, self.bucket_name, f"ingestor_delegate_connections_{self.asOf}", ACL='public-read')
        self.cyphers.connect_delegate_events(urls)
        logging.info(f"successfully ingested delegate connections. good job dev!")

    def enrich_delegate_events(self, data=None):
        logging.info("Enriching delegate events...")
        delegateVotingPowerChanges = data['delegateVotingPowerChanges']
        urls = self.s3.save_df_as_csv(delegateVotingPowerChanges, self.bucket_name, f"ingestor_delegate_voting_power_changes_{self.asOf}", ACL='public-read')
        self.cyphers.enrich_delegation_events(urls)
        logging.info(f"successfully enriched delegate events. good job dev!"

    
    def run(self):
        delegationData = self.prepare_delegation_data()
       #self.ingest_wallets(data=delegationData)
        #self.ingest_delegate_changes(data=delegationData)
        self.ingest_delegate_connections(data=delegationData)
        self.enrich_delegate_events(data=delegationData)
        

if __name__== '__main__':
    ingestor = DelegationIngestor()
    ingestor.run()






        



