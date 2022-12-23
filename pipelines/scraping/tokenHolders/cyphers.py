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