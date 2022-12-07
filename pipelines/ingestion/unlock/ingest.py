

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
    
    def ingest_lock_metadata(self, data=None):
        logging.info("Ingesting lock metadata...")
        lockMetadata = data['lockMetadata']
        urls = self.s3.save_df_as_csv(lockMetadata, self.bucket_name, f"ingestor_locks_{self.asOf}", ACL='public-read') 
        self.cyphers.create_locks(urls)
        logging.info("successfully ingested locks. good job dev!")
    
    def ingest_key_metadata(self, data=None):
        logging.info("Ingesting key metadata...")
        keyMetadata = data['keyMetadata']
        urls = self.s3.save_df_as_csv(keyMetadata, self.bucket_name, f"ingestor_keys_{self.asOf}", ACL='public-read')
        self.cyphers.create_keys(urls)
        logging.info("successfully ingested keys. good job dev")
    
    def ingest_lock_managers(self, data=None):
        logging.info("Ingesting lock managers...")
        lockManagers = data['lockManagers']
        urls = self.s3.save_df_as_csv(lockManagers, self.bucket_name, f"ingestor_lock_managers_{self.asOf}", ACL='public-read')
        self.cyphers.create_lock_managers(urls)
        logging.info("successfully ingested lock managers. good job dev!")

    def ingest_key_holders(self, data=None):
        logging.info("Ingesting key holders...")
        keyHolders = data['keyHolders']
        urls = self.s3.save_df_as_csv(keyHolders, self.bucket_name, f"ingestor_key_holders_{self.asOf}", ACL='public-read')
        self.cyphers.create_key_holders(urls)
        logging.info("successfully ingested key holders. good job dev!")
    
    def ingest_locks_keys(self, data=None):
        logging.info("Connecting locks and keys...")
        locks = data['allTokensUnique']
        urls = self.s3.save_df_as_csv(locks, self.bucket_name, f"ingest_locks_keys{self.asOf}", ACL='public-read')
        self.cyphers.connect_locks_keys(urls)
        logging.info("successfully connected locks to keys. good job dev!")


    def run(self):
        unlockData = self.prepare_unlock_data()
        #self.ingest_wallets(data=unlockData)
       # self.ingest_lock_metadata(data=unlockData)
        self.ingest_key_metadata(data=unlockData)
        self.ingest_lock_managers(data=unlockData)
        self.ingest_key_holders(data=unlockData)
        self.ingest_locks_keys(data=unlockData)



if __name__ == "__main__":
    ingestor = UnlockIngestor()
    ingestor.run()
