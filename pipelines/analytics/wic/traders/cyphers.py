from .. import WICCypher
from ....helpers import Queries, count_query_logging

class TradersCyphers(WICCypher):
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
                MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
                SET con.toRemove = null
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
                MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
                SET con.toRemove = null
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
                MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
                SET con.toRemove = null
                RETURN count(wallet) 
            """
            count += self.query(connect_wallets)[0].value()
        
        return count 

    @count_query_logging
    def connect_x2y2_borrowers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:BORROWER]->(m:Marketplace {{name:"x2y2"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()
        
        return count

    @count_query_logging
    def connect_arcade_borrowers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:BORROWER]->(m:Marketplace {{name:"arcade.xyz"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_paraspace_borrowers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:BORROWER]->(m:Marketplace {{name:"paraspace"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_nftfi_borrowers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:BORROWER]->(m:Marketplace {{name:"nftfi"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_bend_borrowers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:BORROWER]->(m:Marketplace {{name:"bend"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_paraspace_lenders(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:LENDER]->(m:Marketplace {{name:"paraspace"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_x2y2_lenders(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:LENDER]->(m:Marketplace {{name:"x2y2"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_bend_lenders(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:LENDER]->(m:Marketplace {{name:"bend"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_arcade_lenders(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:LENDER]->(m:Marketplace {{name:"arcade.xyz"}})
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_nftfi_lenders(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:LENT]->(m:Loan)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count

    @count_query_logging
    def connect_nftfi_borrowers(self, context):
        query = f"""
        MATCH (wallet:Wallet)-[r:BORROWED]->(m:Loan)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(*)
        """
        count = self.query(query)[0].value()

        return count





    






    

            