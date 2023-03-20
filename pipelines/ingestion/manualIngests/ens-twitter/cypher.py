from ....helpers import count_query_logging
from ....helpers import Queries
from ....helpers import Cypher

class ENSTwitterCyphers(Cypher):
    def __init__(self, database=None):
        self.queries = Queries()
        super().__init__(database)

    @count_query_logging
    def link_or_merge_ens_names(self, urls):
        count = 0 
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS names
                MATCH (twitter:Twitter:Account {{handle: toLower(names.handle)}})
                MATCH (ens:Alias:Ens {{name: toLower(names.ens_name)}})
                MERGE (twitter)-[r:HAS_ALIAS]->(ens)
                SET r.citation = names.source
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_wallets(self, urls):
        count = 0 
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS names
                MATCH (twitter:Twitter:Account {{handle: toLower(names.handle)}})
                MATCH (wallet:Wallet {{address: toLower(names.address)}})
                MERGE (wallet)-[r:HAS_ACCOUNT]->(twitter)
                SET r.citation = names.source
                RETURN count(r)
            """
            count += self.query(query)[0].value()
        return count