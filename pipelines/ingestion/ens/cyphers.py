from tqdm import tqdm
from ...helpers import Cypher, Constraints, Indexes, Queries, count_query_logging


class ENSCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.ens()

    def create_indexes(self):
        indexes = Indexes()
        indexes.ens()

    @count_query_logging
    def create_or_merge_ens_domains(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as domains
                        MERGE (ens:Alias:Ens {{name: toLower(domains.name)}})
                        ON CREATE SET   ens.uuid = apoc.create.uuid(),
                                        ens.labelName = domains.labelName,
                                        ens.createdAt = datetime({{epochSeconds: toInteger(domains.createdAt)}}),
                                        ens.owner = domains.owner,
                                        ens.resolvedAddress = domains.resolvedAddress,
                                        ens.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                        ens.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET    ens.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        RETURN COUNT(DISTINCT(ens))
                        """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_ENS_resolved_addresses(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as domains
                        MATCH (ens:Alias:Ens {{name: toLower(domains.name)}})
                        MATCH (wallet:Wallet {{address: toLower(domains.resolvedAddress)}})
                        MERGE (wallet)-[r:HAS_ALIAS]->(ens)
                        RETURN COUNT(r)
                """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_ENS_owners(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as domains
                        MATCH (ens:Alias:Ens {{name: toLower(domains.name)}})
                        MATCH (wallet:Wallet {{address: toLower(domains.owner)}})
                        MERGE (wallet)-[r:IS_OWNER]->(ens)
                        RETURN COUNT(r)
                """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_ENS_registrations(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as registrations
                        MATCH (ens:Alias:Ens {{name: toLower(registrations.name)}})
                        MATCH (wallet:Wallet {{address: toLower(registrations.owner)}})
                        MERGE (wallet)-[registration:REGISTERED {{txHash: registrations.transactionID}}]->(ens)
                        SET registration.expiryDate = datetime({{epochSeconds: toInteger(registrations.expiryDate)}}),
                            registration.registrantContract = toLower(registrations.registrant),
                            registration.blockNumber = toInteger(registrations.blockNumber)
                        RETURN COUNT(registration)
                """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_ENS_transfers(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as transfers
                        MATCH (fromWallet:Wallet {{address: toLower(transfers.from)}})
                        MATCH (toWallet:Wallet {{address: toLower(transfers.to)}})
                        MERGE (fromWallet)-[transfer:TRANSFERED {{txHash: transfers.transactionID}}]->(toWallet)
                        SET transfer.type = "ENS",
                            transfer.name = toLower(transfers.name),
                            transfer.blockNumber = toInteger(transfers.blockNumber)
                        RETURN COUNT(transfer)
                """
            count += self.query(query)[0].value()
        return count
    
    @count_query_logging
    def link_ENS_burns(self, urls):
        count = 0
        for url in tqdm(urls):
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as transfers
                        MATCH (fromWallet:Wallet {{address: toLower(transfers.from)}})
                        MATCH (ens:Alias:Ens {{name: toLower(transfers.name)}})
                        MERGE (fromWallet)-[burn:BURNED {{txHash: transfers.transactionID}}]->(ens)
                        SET burn.type = "ENS",
                            burn.name = toLower(transfers.name),
                            burn.blockNumber = toInteger(transfers.blockNumber)
                        RETURN COUNT(burn)
                """
            count += self.query(query)[0].value()
        return count