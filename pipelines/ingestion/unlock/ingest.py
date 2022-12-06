

from ..helpers import Ingestor
from .cyphers import UnlockCyphers
import datetime
import pandas as pd
from typing import Dict, List, Any
import logging

class UnlockIngestor(Ingestor):
    def __init__(self):
        self.cyphers = UnlockCyphers()
        super().__init__('unlock-test')

    def prepare_unlock_data(self):
        locksDf = pd.DataFrame(self.scraper_data['locks'][1:])
        keysDf = pd.DataFrame(self.scraper_data['keys'][1:])

        logging.info("Ingested data... there are {lockRows} and {keyRows}".format(
            lockRows = len(locksDf),
            keyRows = len(keysDf)
        ))

        ### object metadata
        lockMetadata = locksDf.drop(columns=['keys', 'lockManagers'])
        keyMetadata = keysDf.filter(['lockAddress', 'keyId', 'expiration', 'tokenURI'])

        ### relationships
        lockManagers = locksDf.filter(['address', 'lockManagers'])
        lockManagers = lockManagers.explode('lockManagers')
        lockManagers = lockManagers.rename(columns={'lockManagers':'lockManager'})
        keyHolders = keysDf.filter(['owner', 'lockAddress', 'keyId'])
        keyHolders['tokenId'] = keyHolders['keyId'].apply(lambda x: x.split('-')[1])

        ### wallets
        lockManagerWallets = list(lockManagers['lockManager'])
        keyHolderWallets = list(keyHolders['owner'])
        allWallets = lockManagerWallets + keyHolderWallets
        allWalletsUnique = list(set(allWallets))
        allWalletsUnique = pd.DataFrame()
        allWalletsUnique['address'] = allWallets

        return {
            'lockMetadata':lockMetadata, 'keyMetadata':keyMetadata, 'lockManagers':lockManagers, 'keyHolders': keyHolders,
            'allWalletsUnique': allWalletsUnique
        }

    def ingest_wallets(self): 
        logging.info("Ingesting wallets...")
        
        unlockData = self.prepare_unlock_data()
        allWalletsUnique = unlockData['allWalletsUnique'] 
        urls = self.s3.save_df_as_csv(allWalletsUnique, self.bucket_name, f"ingestor_wallets_{self.asOf}", ACL='public-read')
        self.cyphers.create_wallets(urls)
        logging.info(f"successfully ingested wallets. good job dev!")
    
    def ingest_lock_metadata(self):
        logging.info("Ingesting lock metadata...")

        unlockData = self.prepare_unlock_data()
        lockMetadata = unlockData['lockMetadata']
        urls = self.s3.save_df_as_csv(lockMetadata, self.bucket_name, f"ingestor_locks_{self.asOf}", ACL='public-read') 
        self.cyphers.create_locks(urls)
        logging.info("successfully ingested locks. good job dev!")

    def run(self):
        self.prepare_unlock_data()
        self.ingest_wallets()
        self.ingest_lock_metadata()

if __name__ == "__main__":
    ingestor = UnlockIngestor()
    ingestor.run()
