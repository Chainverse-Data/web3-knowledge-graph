from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging


class MultisigCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.wallets()

    def create_indexes(self):
        indexes = Indexes()
        indexes.wallets()

    @count_query_logging
    def create_or_merge_multisig_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def add_multisig_labels(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (w:Wallet {{address: wallets.multisig}})
                    SET w:MultiSig,
                        w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        w.threshold = toInteger(wallets.threshold),
                        w.occurDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms'))
                    return count(w)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_multisig_signer(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (m:Wallet {{address: wallets.multisig}}), (s:Wallet {{address: wallets.address}})
                    MERGE (s)-[r:IS_SIGNER]->(m)
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count
