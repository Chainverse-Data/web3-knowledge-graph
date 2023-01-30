from tqdm import tqdm
from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging

class DelegationCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_or_merge_delegations(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
            LOAD CSV WITH HEADERS FROM '{url}' AS delegations
            MERGE (delegation:Delegation {{protocol: delegations.protocol}})
            ON CREATE set delegation.uuid = apoc.create.uuid(),
                    delegation.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    delegation.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    delegation.ingestedBy = "{self.CREATED_ID}"
            ON MATCH set delegation.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                    delegation.ingestedBy = "{self.UPDATED_ID}"
            RETURN COUNT(delegation)
                """
            count += self.query(query)[0].value()
        return count

    def create_or_merge_tokens(self, urls):
        self.queries.create_or_merge_tokens(urls, "ERC20")

    @count_query_logging
    def create_or_merge_entities(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS orgs
                    MERGE (org:Entity {{name: orgs.protocol}})
                    ON CREATE set org.uuid = apoc.create.uuid(),
                        org.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set org.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        org.ingestedBy = "{self.UPDATED_ID}"
                    return count(org)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_strategies(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS strategies
                    MATCH (entity:Entity {{name: strategies.protocol}})
                    MATCH (token:Token {{address: toLower(strategies.contractAddress)}})
                    MERGE (entity)-[edge:HAS_STRATEGY {{delegation: true}}]->(token)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.delegation = true,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.delegation = true,
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_delegation_to_token(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (token:Token {{address: toLower(delegations.contractAddress)}}), (delegation:Delegation {{protocol: delegations.protocol}})
                    MERGE (delegation)-[edge:USE_TOKEN]->(token)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_delegation_to_entity(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS strategies
                    MATCH (entity:Entity {{name: strategies.protocol}})
                    MATCH (delegation:Delegation {{protocol: strategies.protocol}})
                    MERGE (entity)-[edge:HAS_DELEGATION]->(delegation)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_wallet_delegators(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (delegation:Delegation {{protocol: delegations.protocol}})
                    MATCH (wallet:Wallet {{address: toLower(delegations.delegator)}})
                    MERGE (wallet)-[edge:IS_DELEGATING]->(delegation)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_wallet_delegates(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (delegation:Delegation {{protocol: delegations.protocol}}), (wallet:Wallet {{address: toLower(delegations.delegate)}})
                    MERGE (wallet)-[edge:IS_DELEGATE]->(delegation)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.previousBalance = delegations.previousBalance,
                        edge.newBalance = delegations.newBalance,
                        edge.numberVotes = delegations.numberVotes,
                        edge.delegatedVotesRaw = delegations.delegatedVotesRaw,
                        edge.delegatedVotes = delegations.delegatedVotes,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.previousBalance = delegations.previousBalance,
                        edge.newBalance = delegations.newBalance,
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_wallets_delegations(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (delegator:Wallet {{address: toLower(delegations.delegator)}})
                    MATCH (delegate:Wallet {{address: toLower(delegations.delegate)}})
                    MERGE (delegator)-[edge:DELEGATES_TO {{protocol: delegations.protocol}}]->(delegate)
                    ON CREATE set edge.uuid = apoc.create.uuid(),
                        edge.protocol = delegations.protocol,
                        edge.txHash = delegations.txHash,
                        edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        edge.txHash = delegations.txHash,
                        edge.ingestedBy = "{self.UPDATED_ID}"
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count            
            
    @count_query_logging
    def detach_wallets_delegations(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (delegator:Wallet {{address: toLower(delegations.delegator)}})
                    MATCH (delegate:Wallet {{address: toLower(delegations.delegate)}})
                    MATCH (delegator)-[edge:DELEGATES_TO {{protocol: delegations.protocol}}]->(delegate)
                    DELETE edge
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def detach_wallets_delegators(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (delegator:Wallet {{address: toLower(delegations.delegator)}})
                    MATCH (delegation:Delegation {{protocol: delegations.protocol}})
                    MATCH (delegator)-[edge:IS_DELEGATING {{protocol: delegations.protocol}}]->(delegation)
                    DELETE edge
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def detach_wallets_delegates(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS delegations
                    MATCH (delegation:Delegation {{protocol: delegations.protocol}})
                    MATCH (delegate:Wallet {{address: toLower(delegations.delegate)}})
                    MATCH (delegation)-[edge:IS_DELEGATE {{protocol: delegations.protocol}}]->(delegate)
                    DELETE edge
                    return count(edge)
                """
            count += self.query(query)[0].value()
        return count

            
            