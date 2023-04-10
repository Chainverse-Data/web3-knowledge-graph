from .. import WICCypher
from ....helpers import Queries, count_query_logging

class TradersCypher(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        self.queries = Queries()

    @count_query_logging
    def connect_sudo_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS sudo
                MATCH (wallet:Wallet {{address: sudo.seller}}) 
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wallet, wic
                MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
                RETURN count(wallet)
            """
            count += self.query(query)[0].value()

        return count 

    @count_query_logging
    def connect_blur_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS blur
                MATCH (wallet:Wallet {{address: blur.address}}) 
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wallet, wic
                MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
                RETURN count(wallet)
            """
            count += self.query(query)[0].value()

        return count 

    @count_query_logging
    def connect_nft_borrowers(self, context, urls):
        count = 0 
        for url in urls:
            connect_wallets = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS borrower
                MATCH (wallet:Wallet {{address: borrower.address}})
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wallet, wic
                MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
                RETURN count(wallet) 
            """
            count += self.query(connect_wallets)[0].value()
        
        return count 

            