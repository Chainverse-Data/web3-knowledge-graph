from ...helpers import Cypher
from ...helpers import Queries
from ...helpers import count_query_logging


class PropHouseCyphers(Cypher):
    def __init__(self):
        super().__init__()
        self.queries = Queries()

    def create_indexes(self):
        query = """CREATE FULLTEXT INDEX propHouseCommunities IF NOT EXISTS FOR (n:PropHouse) ON EACH [n.name, n.title, n.text]"""
        self.query(query)

    # def create_constraints(self):
        # query = """CREATE CONSTRAINT UniquePropHouseAddress IF NOT EXISTS FOR (d:PropHouse) REQUIRE d.contractAddress IS UNIQUE"""
        # self.query(query)

    @count_query_logging
    def create_wallets(self, urls):
        count = self.queries.create_wallets(urls)
        return count

    @count_query_logging
    def create_communities(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS communities
                MERGE (community:PropHouse:Entity {{communityId: toInteger(communities.id)}})
                ON CREATE set community.uuid = apoc.create.uuid(),
                    community.name = communities.name, 
                    community.profileImageUrl = communities.profileImageURL,
                    community.text = communities.description,
                    community.contractAddress = communities.contractAddress,
                    community.createdDt = datetime(apoc.date.toISO8601(toInteger(communities.createdDate), 'ms')),
                    community.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                ON MATCH set community.name = communities.name, 
                    community.profileImageURL = communities.profileImageURL,
                    community.text = communities.description,
                    community.createdDt = datetime(apoc.date.toISO8601(toInteger(communities.createdDate), 'ms')),
                    community.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                return count(community)
            """
            count += self.query(query)[0].value()
        return count

    def create_tokens(self, urls):
        self.queries.create_or_merge_tokens(urls, "ERC721")

    @count_query_logging
    def link_communities_tokens(self, urls):
        count = 0
        for url in urls: 
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS communities
                MATCH (community:PropHouse:Entity {{communityId: toInteger(communities.id)}})
                MATCH (token:Token:ERC721 {{address: toLower(communities.contractAddress)}})
                MERGE (entity)-[r:HAS_STRATEGY]->(token)
                MERGE (entity)-[r1:HAS_TOKEN]->(token)
                SET r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                SET r1.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                RETURN count(distinct(r))
            """
            count += self.query(query)[0].value()

        return count

    @count_query_logging
    def create_auctions(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS auctions
                    MERGE(auction:PropHouse:Auction {{auctionId: toInteger(auctions.id)}})
                    ON CREATE set auction.uuid = apoc.create.uuid(),
                        auction.title = auctions.title,
                        auction.text = auctions.description,
                        auction.startDt = datetime(apoc.date.toISO8601(toInteger(auctions.startDt), 's')),
                        auction.endDt = datetime(apoc.date.toISO8601(toInteger(auctions.endDt), 's')),
                        auction.fundingAmount = toInteger(auctions.fundingAmount),
                        auction.currency = auctions.currencyType,
                        auction.numWinners = toInteger(auctions.numWinners),
                        auction.createdDt = datetime(apoc.date.toISO8601(toInteger(auctions.createdDate), 'ms')),
                        auction.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set auction.title = auctions.title,
                        auction.text = auctions.description,
                        auction.startDt = datetime(apoc.date.toISO8601(toInteger(auctions.startDt), 's')),
                        auction.endDt = datetime(apoc.date.toISO8601(toInteger(auctions.endDt), 's')),
                        auction.fundingAmount = toInteger(auctions.fundingAmount),
                        auction.currency = auctions.currencyType,
                        auction.numWinners = toInteger(auctions.numWinners),
                        auction.createdDt = datetime(apoc.date.toISO8601(toInteger(auctions.createdDate), 'ms')),
                        auction.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(auction)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_auctions_communities(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' as auctions
                MATCH (auction:PropHouse:Auction {{auctionId: toInteger(auctions.id)}})
                MATCH (entity:PropHouse:Entity {{communityId: toInteger(auctions.communityId)}})
                MERGE (auction)-[r:HAS_AUCTION]->(entity)
                RETURN count(distinct(r))
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_proposals(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                    MERGE(p:PropHouse:Proposal {{proposalId: toInteger(proposals.id)}})
                    ON CREATE set p.uuid = apoc.create.uuid(),
                        p.title = proposals.title,
                        p.text = proposals.tldr,
                        p.createdDt = datetime(apoc.date.toISO8601(toInteger(proposals.createdDate), 'ms')),
                        p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    ON MATCH set p.title = proposals.title,
                        p.text = proposals.tldr,
                        p.createdDt = datetime(apoc.date.toISO8601(toInteger(proposals.createdDate), 'ms')),
                        p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                    return count(p)
            """
            count += self.query(query)[0].value()
        return count

    # here
    @count_query_logging
    def link_auction_proposals(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                    MATCH(p:PropHouse:Proposal {{proposalId: toInteger(proposals.id)}})
                    MATCH(a:PropHouse:Auction {{auctionId: toInteger(proposals.auctionId)}})
                    MERGE(a)-[r:HAS_PROPOSAL]->(p)
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_proposal_wallets(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                    MATCH (p:PropHouse:Proposal {{proposalId: toInteger(proposals.id)}})
                    MATCH (w:Wallet {{address: toLower(proposals.address)}})
                    MERGE (w)-[r:AUTHOR]->(p)
                    return count(r)
            """
            count += self.query(query)[0].value()
        return count

    #here
    @count_query_logging
    def link_proposals_entities(self, urls):
        count = 0
        for url in urls:
            query = f"""
                LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                MATCH (proposal:PropHouse:Proposal {{proposalId: toInteger(proposals.id)}})
                MATCH (community:PropHouse:Entity {{communityId: toInteger(proposals.communityId)}})
                MERGE (community)-[r:HAS_PROPOSAL]->(proposal)
                RETURN count(distinct(r))
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def add_winner_labels(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS winners
                    MATCH (p:PropHouse:Proposal {{proposalId: toInteger(winners.id)}})
                    SET p:Winner
                    return count(p)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_votes(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS votes
                    MATCH (w:Wallet {{address: votes.address}})
                    MATCH (p:PropHouse:Proposal {{proposalId: toInteger(votes.proposalId)}})
                    MERGE (w)-[v:VOTED]->(p)
                    SET v.weight = toInteger(votes.weight),
                        v.direction = toInteger(votes.direction),
                        v.createdDt = datetime(apoc.date.toISO8601(toInteger(votes.createdDate), 'ms'))
                    return count(v)
            """
            count += self.query(query)[0].value()
        return count
