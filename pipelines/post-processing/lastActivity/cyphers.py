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
            RETURN wallet.address
        """
        if DEBUG:
            query += "LIMIT 10"
        result = self.query(query)
        result = [el[0] for el in result]
        return result

    @get_query_logging
    def get_all_wallets_without_first_tx(self):
        query = """
            MATCH (wallet:Wallet)
            WHERE NOT EXISTS (wallet.ethereumLastTxDate) 
            OR NOT EXISTS (wallet.polygonLastTxDate) 
            OR NOT EXISTS (wallet.optimisimLastTxDate) 
            OR NOT EXISTS (wallet.arbitrumLastTxDate) 
            OR NOT EXISTS (wallet.solanaLastTxDate) 
            RETURN wallet.address
        """
        if DEBUG:
            query += "LIMIT 10"
        result = self.query(query)
        result = [el[0] for el in result]
        return result

    @count_query_logging
    def set_last_active_date(self, urls):
        """CSV headers must be: address | ethereum | polygon | optimisim | arbitrum | solana"""
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                MATCH(wallet:Wallet {{address: wallets.address}})

                WITH wallet, wallets,
                CASE wallets.ethereum
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.ethereum}})
                END as ethereumLastTxDate
                SET wallet.ethereumLastTxDate = ethereumLastTxDate

                WITH wallet, wallets,
                CASE wallets.polygon
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.polygon}})
                END as polygonLastTxDate
                SET wallet.polygonLastTxDate = polygonLastTxDate

                WITH wallet, wallets,
                CASE wallets.optimisim
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.optimisim}})
                END as optimisimLastTxDate
                SET wallet.optimisimLastTxDate = optimisimLastTxDate

                WITH wallet, wallets,
                CASE wallets.arbitrum
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.arbitrum}})
                END as arbitrumLastTxDate
                SET wallet.arbitrumLastTxDate = arbitrumLastTxDate

                WITH wallet, wallets,
                CASE wallets.solana
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.solana}})
                END as solanaLastTxDate
                SET wallet.solanaLastTxDate = solanaLastTxDate
                
                return count(wallet)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def set_first_active_date(self, urls):
        """CSV headers must be: address | ethereum | polygon | optimisim | arbitrum | solana"""
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                MATCH(wallet:Wallet {{address: wallets.address}})
                WITH wallet, wallets,
                CASE wallets.ethereum
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.ethereum}})
                END as ethereumFirstTxDate
                SET wallet.ethereumFirstTxDate = ethereumFirstTxDate

                WITH wallet, wallets,
                CASE wallets.polygon
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.polygon}})
                END as polygonFirstTxDate
                SET wallet.polygonFirstTxDate = polygonFirstTxDate

                WITH wallet, wallets,
                CASE wallets.optimisim
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.optimisim}})
                END as optimisimFirstTxDate
                SET wallet.optimisimFirstTxDate = optimisimFirstTxDate

                WITH wallet, wallets,
                CASE wallets.arbitrum
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.arbitrum}})
                END as arbitrumFirstTxDate
                SET wallet.arbitrumFirstTxDate = arbitrumFirstTxDate

                WITH wallet, wallets,
                CASE wallets.solana
                    WHEN NULL THEN null
                    ELSE datetime({{epochmillis: wallets.solana}})
                END as solanaFirstTxDate
                SET wallet.solanaFirstTxDate = solanaFirstTxDate
                
                return count(wallet)
            """
            count += self.query(query)[0].value()
        return count