from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging

class DelegationCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()
    
    @count_query_logging

    def create_or_merge_wallets(self, urls):
        count = 0 
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
            MERGE (w:Wallet {{address: wallets.address}})
            ON MATCH 
                SET
                    w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    w:DelegationTest
            ON CREATE
                SET
                    w.uuid = apoc.create.uuid(),
                    w:DelegationTest,
                    w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    w.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
            RETURN
                count(distinct(w))
            """
            count += self.query(query)[0].value()
        return count

    def create_delegation_events(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS delegations
            MERGE (d:Delegation:Transaction {{eventId: delegations.id}})
            ON MATCH
                SET
                    d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
            ON CREATE
                SET
                    d.blockNumber = toInteger(delegations.blockNumber),
                    d.blockTimestamp = toInteger(delegations.blockTimestamp),
                    d.logIndex = delegations.logIndex,
                    d:DelegationTest,
                    d.txHash = delegations.txnHash
            RETURN 
                COUNT(d)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def connect_delegate_events(self, urls):
        count = 0
        for url in urls:
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS delegations
            MATCH (d:Delegation:Transaction {{eventId: delegations.id}})
            MATCH (delegate:Wallet {{address: delegations.delegate}})
            MATCH (delegator:Wallet {{address: delegations.delegator}})
            WITH d,delegate, delegator
            MERGE (delegator)-[r:DELEGATED]->(d)
            MERGE (d)-[r1:DELEGATED_TO]->(delegate)
            ON CREATE
                SET 
                    r.uuid = apoc.create.uuid(),
                    r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    r1.uuid = apoc.create.uuid(),
                    r1.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    r1.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
            ON MATCH
                SET 
                    r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    r1.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
            RETURN 
                COUNT(*)
            """
            count += self.query(query)[0].value()
        return count

            
            