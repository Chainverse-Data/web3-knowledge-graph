from ...helpers import Cypher
from ...helpers import get_query_logging

class TokenHoldersCypher(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_all_wallets(self):
        query = """
            MATCH wallet:Wallet
            RETURN wallet.address
        """
        result = self.query(query)
        return result