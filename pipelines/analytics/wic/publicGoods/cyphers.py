from .. import WICCypher
from ....helpers import count_query_logging

class EcoDevCyphers(WICCypher):
    def __init__(self, subgraph_name, conditions, database=None):
        WICCypher.__init__(self, subgraph_name, conditions, database)
    
    def get_grant_donation_benchmark(self):
        benchmark_query = f"""
            MATCH (wallet:Wallet)-[r:DONATION]->(g:Grant)
            WITH wallet, count(distinct(g)) AS donations
            WITH apoc.agg.percentiles(donations, [.5]) AS percentile
            RETURN tofloat(percentile[0]) AS benchmark
        """
        benchmark = self.query(benchmark_query)[0].value()
        return benchmark

    @count_query_logging
    def connect_gitcoin_grant_donors(self, context):
        connect_query = f"""
            WITH {benchmark} AS benchmark
            MATCH (wallet:Wallet)-[r:DONATION]->(g:Grant)
            WITH wallet, count(distinct(g)) AS donations
            WHERE donations > 2
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_query)[0].value()
        return count 

    def get_grant_admin_benchmark(self):
        benchmark_query = """
            MATCH (wallet:Wallet)-[r:IS_ADMIN|MEMBER_OF]->(grant:Grant)
            WITH wallet, count(distinct(grant)) AS grants_admin
            WITH apoc.agg.percentiles(grants_admin, [.5]) AS percentile
            RETURN tofloat(percentile[0]) AS benchmark
        """
        benchmark = self.query(benchmark_query)[0].value()
        return benchmark

    @count_query_logging
    def connect_gitcoin_grant_admins(self, context, benchmark):
        connect_query = f"""
            WITH {benchmark} AS benchmark 
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet:Wallet)-[:IS_ADMIN]-(grant:Grant)
            WITH wallet, count(distinct(grant)) AS grants_admin, wic, benchmark
            WITH wallet, wic, (tofloat(grants_admin) / benchmark) AS againstBenchmark
            MATCH (wallet)
            MATCH (wic)
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._againstBenchmark = againstBenchmark
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_query)[0].value()
        return count

    @count_query_logging
    def connect_grants_daos(self, context, daos):
        count = 0
        for name in daos:
            connect_paradigm = f"""
                MATCH (w:_Wic:_{self.subgraph_name}:_Context:_{context})
                MATCH (e:Entity)
                WHERE e.name CONTAINS '{name}'
                WITH e,w 
                MATCH (e)
                MATCH (w)
                MERGE (w)-[r:_PARADIGM_CASE]->(e)
                RETURN count(distinct(e))
            """
            count += self.query(connect_paradigm)[0].value()
        return count 
    
    @count_query_logging
    def connect_grant_dao_wallets(self, context):
        connect_wallets = f"""
            MATCH (wallet:Wallet)-[r:VOTED]->(p:Proposal)-[:HAS_PROPOSAL]-(e:Entity)-[:_PARADIGM_CASE]-(wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, count(distinct(e)) AS ents, wic
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._count = ents
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_wallets)[0].value()
        return count

    def get_gitcoin_bounty_creator_benchmark(self):
        benchmark_query = """
            MATCH (bounty:Bounty:Gitcoin)-[:IS_OWNER]-(g:Account:Github)-[:HAS_ACCOUNT]-(wallet:Wallet)
            WITH wallet, count(distinct(bounty)) AS bounties
            RETURN apoc.agg.percentiles(bounties, [.5])[0] AS benchmark
        """
        benchmark = self.query(benchmark_query)[0].value()
        return benchmark

    @count_query_logging
    def connect_gitcoin_bounty_creators(self, context, benchmark):
        connect_query = f"""
            WITH tofloat({benchmark}) AS benchmark
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (bounty:Bounty:Gitcoin)-[:IS_OWNER]-(g:Account:Github)-[:HAS_ACCOUNT]-(wallet:Wallet)
            WITH wallet, count(distinct(bounty)) AS bounties, wic, benchmark
            WITH wallet, wic, (tofloat(bounties) / benchmark) AS againstBenchmark
            MATCH (wallet)
            MATCH (wic)
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._againstBenchmark = againstBenchmark
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_query)[0].value()
        return count

    def get_gitcoin_bounty_fullfilers_benchmark(self):
        benchmark_query = """
            MATCH (bounty:Bounty:Gitcoin)-[:HAS_FULLFILLED]-(g:Account:Github)-[:HAS_WALLET]-(wallet:Wallet)
            WITH wallet, count(distinct(bounty)) as bounties
            RETURN apoc.agg.percentiles(bounties, [.5])[0] as benchmark
        """
        benchmark = self.query(benchmark_query)[0].value()
        return benchmark

    @count_query_logging
    def connect_gitcoin_bounty_fulfillers(self, context, benchmark):
        connect_query = f"""
            WITH tofloat({benchmark}) AS benchmark
            MATCH (bounty:Bounty:Gitcoin)-[:HAS_FULLFILLED]-(g:Account:Github)-[:HAS_WALLET]-(wallet:Wallet)
            WITH wallet, count(distinct(bounty)) AS bounties, benchmark
            WITH wallet, bounties, benchmark 
            WITH wallet, (tofloat(bounties) / benchmark) AS againstBenchmark
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            MATCH (wallet)
            WITH wic, wallet, againstBenchmark
            MATCH (wallet)
            MATCH (wic)
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            SET con._againstBenchmark = againstBenchmark
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_query)[0].value()
        return count 

    @count_query_logging
    def connect_incubators(self, context, incubators):
        count = 0
        for incubator in incubators:
            query = f"""
                MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
                MATCH (e:Entity)
                WHERE e.name contains '{incubator}'
                WITH wic, e
                MATCH (wic)
                MATCH (e)
                MERGE (wic)-[r:_PARADIGM_CASE]->(e)
                RETURN count(distinct(e))
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_incubators_members(self, root_context, context):
        connect_affiliates_voted = f"""
            MATCH (wallet:Wallet)-[:VOTED]-(p:Proposal)-[:HAS_PROPOSAL]-(e)-[:_PARADIGM_CASE]-(:_Context:_{root_context})
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, wic
            MATCH (wallet)
            MATCH (wic)
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_affiliates_voted)[0].value
        return count

    @count_query_logging
    def connect_incubators_participant(self, root_context, context):
        connect_participants_voted = f"""
            MATCH (wallet:Wallet)-[:VOTED]-(:Proposal)-[]-(incubated:Entity)<-[:INCUBATED]-(incubator:Entity)
            MATCH (wic:_Wic:_{self.subgraph_name}:_Context:_{context})
            WITH wallet, wic
            MATCH (wallet)
            MATCH (wic)
            MERGE (wallet)-[con:_HAS_CONTEXT]->(wic)
            SET con.toRemove = null
            RETURN count(distinct(wallet))
        """
        count = self.query(connect_participants_voted)[0].value()
        return count 

        
