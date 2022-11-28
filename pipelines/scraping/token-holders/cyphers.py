from ...helpers import Cypher
from ...helpers import get_query_logging

class TokenHoldersCypher(Cypher):
    def __init__(self, database=None):
        super().__init__(database)

    @get_query_logging
    def get_all_wallets(self):
        query = """
            MATCH (wallet:Wallet)
            RETURN wallet.address
            LIMIT 20
        """
        result = self.query(query)
        result = [el[0] for el in result]
        return result