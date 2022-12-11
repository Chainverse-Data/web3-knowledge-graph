from ..helpers import Ingestor
from .cyphers import DelegationCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging

class DelegationIngestor(Ingestor):
    def __init__(self):
        self.cyphers = DelegationCyphers()
        super().__init__('delegation-test')
class UnlockIngestor(Ingestor):
    def __init__(self):
        self.cyphers = DelegationCyphers()
        super().__init__('unlock-test')
    def prepare_delegation_data(self):
        delegateChangesDf = pd.read_csv(self.scraper_data['delegateChanges'][1:])
        delegateVotingPowerChanges = pd.read_csv(self.scraper_data['delegateVotingPowerChanges'][1:])
        delegatesDf = pd.read_csv(self.scraper_data['delegates'][1:])
        tokenHoldersDf = pd.read_csv(self.scraper_data['tokenHolders'][1:])
        logging.info("""
        Ingested data... there are {lenDelegateChanges} delegate changes, 
        {lenDelegateVotingPowerChanges} delegate voting power changes,
        {lenDelegates} delegates, and {lenTokenHolders} token holders
        Let's get it!
        LETS FUCKING GO
        """).format(
            lenDelegateChanges = len(delegateChangesDf),
            lenDelegateVotingPowerChanges = len(delegateVotingPowerChanges),
            lenDelegates = len(delegatesDf),
            lenTokenHolders = len(tokenHoldersDf)
        )
        ## make wallet list
        delegateWallets = list(delegateChangesDf['delegate'])
        delegatorWallets = list(delegateChangesDf['delegator'])
        tokenHoldersDf = list(tokenHoldersDf['tokenHolderAddress'])
        allWallets = list(set(list(delegateWallets + delegatorWallets + tokenHoldersDf)))
        allWalletsDf = pd.DataFrame()
        allWalletsDf['address'] = allWallets

        ## make delegation events
        delegationEventsDf = pd.DataFrame()


        




        logging.info("Ingested data... there are {lockRows} and {keyRows}".format(


        ### object metadata
        lockMetadata = locksDf.drop(columns=['keys', 'lockManagers'])
        keyMetadata = keysDf.filter(['lockAddress', 'keyId', 'expiration', 'tokenURI'])

        ### relationships
        lockManagers = locksDf.filter(['address', 'lockManagers'])
        lockManagers = lockManagers.explode('lockManagers')
        lockManagers = lockManagers.rename(columns={'lockManagers':'lockManager', 'address': 'lockAddress'})
        keyHolders = keysDf.filter(['owner', 'lockAddress', 'keyId'])
        keyHolders['tokenId'] = keyHolders['keyId'].apply(lambda x: x.split('-')[1])

        ### wallets
        lockManagerWallets = list(lockManagers['lockManager'])
        keyHolderWallets = list(keyHolders['owner'])
        allWallets = lockManagerWallets + keyHolderWallets
        allWalletsUnique = list(set(allWallets))
        allWalletsUnique = pd.DataFrame()
        allWalletsUnique['address'] = allWallets
        

        ## tokens
        tokensUnique = list(set(locksDf.filter(['lockAddress'])))
        allTokensUnique = pd.DataFrame(data=tokensUnique, columns=['address'])


        return {
            'lockMetadata':lockMetadata, 'keyMetadata':keyMetadata, 'lockManagers':lockManagers, 'keyHolders': keyHolders,
            'allWalletsUnique': allWalletsUnique, 'allTokensUnique': allTokensUnique
        } 

    def ingest_wallets(self, data=None): 
        logging.info("Ingesting wallets...")
        allWalletsUnique = data['allWalletsUnique'] 
        urls = self.s3.save_df_as_csv(allWalletsUnique, self.bucket_name, f"ingestor_wallets_{self.asOf}", ACL='public-read')
        self.cyphers.create_wallets(urls)
        logging.info(f"successfully ingested wallets. good job dev!")
