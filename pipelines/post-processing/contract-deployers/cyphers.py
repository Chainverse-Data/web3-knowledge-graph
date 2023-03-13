

from ...helpers import Queries
from ...helpers import get_query_logging, count_query_logging
from ...helpers import Cypher
import os

DEBUG = os.environ.get("DEBUG", False)

class ContractDeployersCyphers(Cypher):
    def __init__(self, database=None):
        self.queries = Queries()
        super().__init__(database)

    def create_indexes(self):
        query = "CREATE INDEX DeployedTx IF NOT EXISTS FOR ()-[r:DEPLOYED]->() ON (r.txHash)"
        self.query(query)

    @get_query_logging
    def get_multisigs(self):
        query = f"""
            MATCH (multisig:MultiSig)
            WHERE NOT (:Wallet)-[:DEPLOYED]->(multisig)
            RETURN multisig.address as address
        """
        if DEBUG:
            query += "LIMIT 10"
        results = self.query(query)
        results = [result["address"] for result in results]
        return results

    @get_query_logging
    def get_tokens(self):
        query = f"""
            MATCH (token:Token)
            WHERE NOT (:Wallet)-[:DEPLOYED]->(token)
            RETURN token.address as address
        """
        if DEBUG:
            query += "LIMIT 10"
        results = self.query(query)
        results = [result["address"] for result in results]
        return results

    @count_query_logging
    def link_or_merge_deployers(self, urls, label):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS deployers
                MATCH (wallet:Wallet {{address: toLower(deployers.address)}})
                MATCH (contract:{label} {{address: toLower(deployers.contractAddress)}})
                MERGE (wallet)-[link:DEPLOYED {{txHash: deployers.txHash}}]->(contract)
                SET link.citation = "Etherescan API"
                RETURN count(link)
            """
            count += self.query(query)[0].value()
        return count