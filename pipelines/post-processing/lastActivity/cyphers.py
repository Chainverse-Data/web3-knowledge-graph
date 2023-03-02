from datetime import datetime

from tqdm import tqdm
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging
import os

DEBUG = os.environ.get("DEBUG", False)

class LastActivityCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_all_wallets(self):
        query = """
            MATCH (wallet:Wallet)
            RETURN wallet.address as address
        """
        if DEBUG:
            query += "LIMIT 10"
        result = self.query(query)
        return result

    @get_query_logging
    def get_all_wallets_without_first_tx(self):
        query = """
            MATCH (wallet:Wallet)
            WHERE wallet.ethereumFirstTxDate IS NULL
            OR wallet.polygonFirstTxDate IS NULL 
            OR wallet.optimisimFirstTxDate IS NULL 
            OR wallet.arbitrumFirstTxDate IS NULL 
            RETURN  wallet.address as address, 
                    wallet.ethereumFirstTxDate as ethereum,
                    wallet.polygonFirstTxDate as polygon,
                    wallet.optimisimFirstTxDate as optimisim,
                    wallet.arbitrumFirstTxDate as arbitrum
        """
        if DEBUG:
            query += "LIMIT 10"
        result = self.query(query)
        return result
    
    @count_query_logging
    def set_last_active_date(self, urls, chain):
        """CSV headers must be: address | date"""
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                MATCH(wallet:Wallet {{address: toLower(wallets.address)}})
                SET wallet.{chain}LastTxDate = datetime({{epochSeconds: toInteger(wallets.date)}})
                return count(wallet)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def set_first_active_date(self, urls, chain):
        """CSV headers must be: address | date"""
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                MATCH(wallet:Wallet {{address: toLower(wallets.address)}})
                SET wallet.{chain}FirstTxDate = datetime({{epochSeconds: toInteger(wallets.date)}})
                return count(wallet)
            """
            count += self.query(query)[0].value()
        return count