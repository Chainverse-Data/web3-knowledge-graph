from .. import WICCypher
from ....helpers import count_query_logging, get_query_logging
import time

class DiversityCyphers(WICCypher):

    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
        
    def create_indexes(self):
        query = """CREATE FULLTEXT INDEX wicArticles IF NOT EXISTS FOR (n:Article) ON EACH [n.title, n.text]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicGrants IF NOT EXISTS FOR (n:Grant) ON EACH [n.title, n.text]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicProposals IF NOT EXISTS FOR (n:Proposal) ON EACH [n.title, n.text]"""
        self.query(query)
        query = """CREATE FULLTEXT INDEX wicTwitter IF NOT EXISTS FOR (n:Twitter) ON EACH [n.bio]"""
        self.query(query)

    @count_query_logging
    def connect_authors_to_articles(self, context):
        connect_authors = f"""
            MATCH (w:Wallet)-[r:AUTHOR]->(article:Article)-[:_HAS_CONTEXT]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (w)-[context:_HAS_CONTEXT]->(wic)
            SET context._context = "Author"
            RETURN count(distinct(w))"""
        count = self.query(connect_authors)[0].value()
        return count

    @count_query_logging
    def connect_collectors_to_articles(self, context):
        connect_collectors = f"""
            MATCH (article)-[:_HAS_CONTEXT]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (collector)-[:HOLDS]-(nft:ERC721)-[:HAS_NFT]-(article)
            WITH collector, wic
            MERGE (collector)-[context:_HAS_CONTEXT]->(wic)
            SET context._context = "Collector"
            RETURN count(distinct(collector))
        """
        count = self.query(connect_collectors)[0].value()
        return count

    @count_query_logging
    def connect_articles(self, context, keywords):
        count = 0
        for keyword in keywords:
            identify_query = f"""
                CALL db.index.fulltext.queryNodes("wicArticles", "{keyword}", {{limit: 100000}}) 
                YIELD node, score
                WITH node, score
                ORDER BY score desc
                LIMIT 500
                UNWIND node as article
                MATCH (article:Article)-[:AUTHOR]-(w:Wallet)
                WHERE NOT (w)-[:_HAS_CONTEXT]-(:_IncentiveFarming)
                AND NOT (article)-[:_HAS_CONTEXT]-(:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH article
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WITH wic, article
                MERGE (article)-[r:_HAS_CONTEXT]->(wic)
                SET r._query = "{keyword}"
                RETURN count(distinct(article))
            """
            count += self.query(identify_query)[0].value()
        return count

    @count_query_logging
    def connect_donors_to_grants(self, context):
        connect_donors = f"""
            MATCH (grant:Grant)-[:_HAS_CONTEXT]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[r:DONATION]->(grant)
            WHERE NOT (wallet)-[:_HAS_CONTEXT]->(wic)
            WITH wallet, wic
            MERGE (wallet)-[context:_HAS_CONTEXT]->(wic)
            SET context._context = "Donor"
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_donors)[0].value()
        return count
    
    @count_query_logging
    def connect_admins_to_grants(self, context):
        connect_admin = f"""
            MATCH (grant:Grant)-[:_HAS_CONTEXT]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[r:IS_ADMIN|MEMBER_OF]-(grant)
            WHERE NOT (wallet)-[:_HAS_CONTEXT]-(wic)
            WITH wallet, wic
            MERGE (wallet)-[context:_HAS_CONTEXT]->(wic)
            SET context._context = "GrantAdmin"
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_admin)[0].value()
        return count

    @count_query_logging
    def connect_grants(self, context, keywords):
        count = 0
        for keyword in keywords:
            identify_grants = f"""
                CALL db.index.fulltext.queryNodes("wicGrants", "{keyword}", {{limit: 10000}}) 
                YIELD node, score
                WITH node, score
                ORDER BY score DESC
                LIMIT 50
                UNWIND node as grant
                MATCH (grant:Grant)
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WHERE NOT (grant)-[:_HAS_CONTEXT]->(wic)
                WITH grant, wic
                MERGE (grant)-[context:_HAS_CONTEXT]->(wic)
                SET context._query = "{keyword}"
                RETURN count(distinct(grant))
            """
            count += self.query(identify_grants)[0].value()
        return count 

    @count_query_logging
    def connect_authors_to_proposals(self, context):
        connect_authors = f"""
            MATCH (wallet:Wallet)-[r:AUTHOR]->(p:Proposal)-[:_HAS_CONTEXT]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WHERE NOT (wallet)-[:_HAS_CONTEXT]-(wic)
            WITH wallet, wic
            MERGE (wallet)-[context:_HAS_CONTEXT]->(wic)
            SET context._context = "ProposalAuthor"
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_authors)[0].value()
        return count

    @count_query_logging
    def connect_voters_to_proposals(self, context):
        connect_voters = f"""
            MATCH (wallet:Wallet)-[:VOTED]->(p:Proposal)-[:_HAS_CONTEXT]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WHERE NOT (wallet)-[:_HAS_CONTEXT]->(wic)
            WITH wallet, wic
            MERGE (wallet)-[r:_HAS_CONTEXT]->(wic)
            SET r._context = "Voter"
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_voters)[0].value()
        return count

    @count_query_logging
    def connect_proposals(self, context, keywords):
        count = 0 
        for keyword in keywords:
            relevant_proposal = f"""
                CALL db.index.fulltext.queryNodes("wicProposals", "{keyword}", {{limit: 50000}}) 
                YIELD node, score
                WITH node, score
                ORDER BY score DESC
                LIMIT 100
                UNWIND node as proposal
                MATCH (proposal:Proposal)
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                WHERE NOT (proposal)-[:_HAS_CONTEXT]-(wic)
                WITH proposal, wic
                MERGE (proposal)-[context:_HAS_CONTEXT]->(wic)
                SET context._query = "{keyword}"
                RETURN COUNT(DISTINCT(proposal))
            """
            count += self.query(relevant_proposal)[0].value()
        return count

    @count_query_logging
    def connect_proposals_to_entities(self, context):
        query = f"""
            MATCH (p:Proposal)-[:_HAS_CONTEXT]-(wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)
            WITH e, wic
            MERGE (e)-[:_HAS_CONTEXT]->(wic)
            RETURN count(distinct(e))
        """
        count = self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_bio(self, context, keywords):
        count = 0 
        for keyword in keywords:
            query = f"""
            CALL db.index.fulltext.queryNodes("wicTwitter", "{keyword}", {{limit: 10000}}) 
            YIELD node, score
            WITH node, score
            ORDER BY score DESC
            UNWIND node AS account
            MATCH (account)-[:HAS_ALIAS]-(:Alias)-[:HAS_ALIAS]-(w:Wallet)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WHERE NOT (w)-[:_HAS_CONTEXT]-(wic)
            AND NOT (w)-[:_HAS_CONTEXT]-(:_IncentiveFarming)
            MERGE (w)-[context:_HAS_CONTEXT]->(wic)
            SET context._query = "{keyword}"
            RETURN count(distinct(w))
            """
            count += self.query(query)[0].value()
        return count

    
