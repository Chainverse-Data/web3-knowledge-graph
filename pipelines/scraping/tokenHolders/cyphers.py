from ...helpers import Cypher
from ...helpers import get_query_logging
import os

DEBUG = os.environ.get("DEBUG", False)

class TokenHoldersCypher(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_all_wallets(self):
        if DEBUG:
            query = """
                MATCH (wallet:Wallet)
                RETURN wallet.address
                LIMIT 100
            """
        else:
            query = """
                MATCH (wallet:Wallet)
                RETURN wallet.address
            """
        result = self.query(query)
        result = [el[0] for el in result]
        return result

    @get_query_logging
    def get_important_wallets(self):
        query = """
            match (w:Wallet:MultiSig)
            optional match (w)-[r:_HAS_CONTEXT]-(wic)
            with w, count(distinct(r)) as contexts
            return distinct w.address as address, contexts
            limit 2000
        """
        result = self.query(query)
        wallets = [el["address"] for el in result]
        query = """
            match (w:Wallet)-[r:_HAS_CONTEXT]->()
            where not w:MultiSig 
            and not (w)-[:_HAS_CONTEXT]->(:_IncentiveFarming)
            with w, count(distinct(r)) as contexts
            return distinct w.address as address, contexts
            order by contexts desc
            limit 98000
        """
        result = self.query(query)
        wallets += [el["address"] for el in result]
        return wallets
