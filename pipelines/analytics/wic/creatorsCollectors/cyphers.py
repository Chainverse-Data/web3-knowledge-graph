from .. import WICCypher
from ....helpers import count_query_logging
import logging 

class CreatorsCollectorsCypher(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)


    @count_query_logging
    def cc_writers(self, context):
        label = """
        MATCH (wallet:Wallet)-[r:AUTHOR]->(a:Mirror)
        MATCH (wallet:Wallet)-[:_HAS_CONTEXT]->(wic:_Wic:_Context)
        WHERE NOT (wallet)-[:_HAS_CONTEXT]->(:_Farmers)
        WITH wallet, count(distinct(wic)) as wics
        WHERE wics >= 1
        WITH wallet
        MATCH (wallet:Wallet)-[r:AUTHOR]->(a:Mirror)
        WITH wallet, count(distinct(a)) as arts
        WHERE arts >= 2
        SET wallet:MirrorAuthor"""

        self.query(label)

        connect = f"""
        MATCH (wallet:MirrorAuthor)
        MATCH (wic:_Wic:_Context:_{self.subgraph_name}:_{context})
        WITH wallet, wic
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))
        """
        count = self.query(connect)[0].value()
        return count 

    @count_query_logging
    def get_writing_nft_collectors(self, context):
        label = """
        MATCH (writer:MirrorAuthor)-[r:AUTHOR]->(a:Mirror)-[:HAS_NFT]-(:ERC721)-[:HOLDS_TOKEN]-(collector:Wallet)
        WITH collector, count(distinct(a)) as arts
        SET collector:MirrorCollector"""
        self.query(label)

        connect = f"""
        MATCH (wic:_Context:_Wic_{self.subgraph_name}:_{context})
        MATCH (wallet:MirrorCollector)
        WITH wic, wallet
        MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
        RETURN COUNT(DISTINCT(wallet))"""
        count = self.query(connect)[0].value()

        return count
    
    @count_query_logging
    def cc_blue_chip(self, addresses, context):
        connect = f"""
        MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        MATCH (wallet:Wallet)-[r:HOLDS]->(token:Token)
        WHERE (token.address IN {addresses} OR token.contractAddress IN {addresses})
        WITH wallet, wic, count(distinct(token)) as count_collections
        MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
        SET r.count = count_collections
        RETURN count(distinct(wallet)) AS count
        """
        count = self.query(connect)[0].value()
        return count 

    @count_query_logging
    def three_letter_ens(self, context):
        query = f"""
        match 
            (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
        match 
            (wallet:Wallet)-[:HAS_ALIAS]-(alias:Alias:Ens)
        with 
            wallet, split(alias.name, ".eth")[0] as ens_name, wic
        where 
            size(ens_name) = 3 
        with 
            wallet, wic 
        merge
            (wallet)-[con:_HAS_CONTEXT]->(wic)
        return
            count(con)
        """
        count = self.query(query)[0].value() 

        return count
    @count_query_logging
    def create_sudo_power_users(self, urls):
        count = 0
        for url in urls:
            create_wallets = f"""
            load csv with headers from '{url}' as sudo
            merge (wallet:Wallet {{address: sudo.address}})
            return count(wallet)
            """
            count += self.query(create_wallets)[0].value()

        return count

    @count_query_logging
    def connect_sudo_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
            load csv with headers from '{url}' as sudo
            with collect(distinct(sudo.address)) as addresses
            match (wallet:Wallet) 
            where wallet.address in addresses
            with wallet
            match (wallet)
            match (wic:_Wic:_{self.subgraph_name}:_{context})
            with wallet, wic
            merge (wallet)-[r:_HAS_CONTEXT]->(wic)
            return count(wallet)
            """
            logging.info(query)
            count += self.query(query)[0].value()

        return count 

    @count_query_logging
    def create_blur_power_users(self, urls):
        count = 0
        for url in urls:
            create_wallets = f"""
            load csv with headers from '{url}' as blur
            merge (wallet:Wallet {{address: blur.address}})
            return count(distinct(wallet))
            """
            logging.info(create_wallets)
            count += self.query(create_wallets)[0].value()
        return count

    @count_query_logging
    def connect_blur_power_users(self, context, urls):
        count = 0
        for url in urls: 
            query = f"""
            load csv with headers from '{url}' as blur
            with collect(distinct(blur.address)) as addresses
            match (wallet:Wallet) 
            where wallet.address in addresses
            with wallet
            match (wallet)
            match (wic:_Wic:_{self.subgraph_name}:_{context})
            with wallet, wic
            merge (wallet)-[r:_HAS_CONTEXT]->(wic)
            return count(wallet)
            """
            logging.info(query)
            count += self.query(query)[0].value()

        return count 
        
    @count_query_logging
    def create_nft_borrowers(self, context, urls):
        count = 0 
        for url in urls:
            create_wallets = f"""
            load csv with headers from '{url}' as borrower
            merge (wallet:Wallet {{address: borrower.address}})
            return count(distinct(wallet))
            """
            logging.info(create_wallets)
            count += self.query(create_wallets)[0].value()
    @count_query_logging
    def connect_nft_borrowers(self, context, urls):
        count = 0 
        for url in urls:
            connect_wallets = f"""
            load csv with headers from '{url}' as borrower
            with collect(distinct(borrower.address)) as addresses
            match (wallet:Wallet) 
            where wallet.address in addresses
            with wallet
            match (wallet)
            match (wic:_Wic:_{self.subgraph_name}:_{context})
            with wallet, wic
            merge (wallet)-[r:_HAS_CONTEXT]->(wic)
            return count(wallet) 
            """
            logging.info(connect_wallets)
            count += self.query(connect_wallets)[0].value()
        
        return count 
