from ...helpers import Cypher
from ...helpers import Constraints, Indexes, Queries
from ...helpers import count_query_logging

class DaoHausCyphers(Cypher):
    def __init__(self, database=None):
        super().__init__(database)
        self.queries = Queries()

    def create_constraints(self):
        constraints = Constraints()
        constraints.tokens()
        constraints.daohaus_dao()
        constraints.daohaus_proposal()

    def create_indexes(self):
        indexes = Indexes()
        indexes.tokens()
        indexes.daohaus_dao()
        indexes.daohaus_proposal()

    @count_query_logging
    def create_or_merge_daos(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS daos
                    MERGE(dao:DaoHaus:Dao:Entity {{id: daos.id}})
                    ON CREATE set dao.uuid = apoc.create.uuid(),
                        dao.chain = daos.chain,
                        dao.title = daos.title,
                        dao.summoningTime = daos.summoningTime,
                        dao.periodDuration = daos.periodDuration,
                        dao.votingPeriodLength = daos.votingPeriodLength,
                        dao.gracePeriodLength = daos.gracePeriodLength,
                        dao.proposalDeposit = daos.proposalDeposit,
                        dao.chainId = daos.chainId,
                        dao.deleted = daos.deleted,
                        dao.profileUrl = daos.profileUrl,
                        dao.totalLoot = daos.totalLoot,
                        dao.totalShares = daos.totalShares,
                        dao.version = daos.version,
                        dao.asOf = daos.asOf,
                        dao.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        dao.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        dao.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set dao.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        dao.deleted = daos.deleted,
                        dao.totalLoot = daos.totalLoot,
                        dao.totalShares = daos.totalShares,
                        dao.periodDuration = daos.periodDuration,
                        dao.votingPeriodLength = daos.votingPeriodLength,
                        dao.gracePeriodLength = daos.gracePeriodLength,
                        dao.proposalDeposit = daos.proposalDeposit,
                        dao.asOf = daos.asOf,
                        dao.ingestedBy = "{self.UPDATED_ID}"
                    return count(dao)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_proposals(self, urls):
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                    MERGE(proposal:DaoHaus:Proposal {{id: proposals.id}})
                    ON CREATE set proposal.uuid = apoc.create.uuid(),
                        proposal.createdAt = proposals.createdAt,
                        proposal.createdBy = proposals.createdBy,
                        proposal.proposalId = proposals.proposalId,
                        proposal.molochAddress = proposals.molochAddress,
                        proposal.memberAddress = proposals.memberAddress,
                        proposal.applicant = proposals.applicant,
                        proposal.proposer = proposals.proposer,
                        proposal.sponsor = proposals.sponsor,
                        proposal.processor = proposals.processor,
                        proposal.sharesRequested = proposals.sharesRequested,
                        proposal.lootRequested = proposals.lootRequested,
                        proposal.tributeOffered = proposals.tributeOffered,
                        proposal.tributeToken = proposals.tributeToken,
                        proposal.tributeTokenSymbol = proposals.tributeTokenSymbol,
                        proposal.tributeTokenDecimals = proposals.tributeTokenDecimals,
                        proposal.paymentRequested = proposals.paymentRequested,
                        proposal.paymentToken = proposals.paymentToken,
                        proposal.paymentTokenSymbol = proposals.paymentTokenSymbol,
                        proposal.paymentTokenDecimals = proposals.paymentTokenDecimals,
                        proposal.startingPeriod = proposals.startingPeriod,
                        proposal.sponsored = proposals.sponsored,
                        proposal.sponsoredAt = proposals.sponsoredAt,
                        proposal.processed = proposals.processed,
                        proposal.processedAt = proposals.processedAt,
                        proposal.didPass = proposals.didPass,
                        proposal.cancelled = proposals.cancelled,
                        proposal.cancelledAt = proposals.cancelledAt,
                        proposal.aborted = proposals.aborted,
                        proposal.whitelist = proposals.whitelist,
                        proposal.guildkick = proposals.guildkick,
                        proposal.newMember = proposals.newMember,
                        proposal.trade = proposals.trade,
                        proposal.details = proposals.details,
                        proposal.votingPeriodStarts = proposals.votingPeriodStarts,
                        proposal.votingPeriodEnds = proposals.votingPeriodEnds,
                        proposal.gracePeriodEnds = proposals.gracePeriodEnds,
                        proposal.uberHausMinionExecuted = proposals.uberHausMinionExecuted,
                        proposal.executed = proposals.executed,
                        proposal.minionAddress = proposals.minionAddress,
                        proposal.isMinion = proposals.isMinion,
                        proposal.minionExecuteActionTx = proposals.minionExecuteActionTx,
                        proposal.asOf = proposals.asOf,
                        proposal.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        proposal.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        proposal.ingestedBy = "{self.CREATED_ID}"
                    ON MATCH set proposal.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        proposal.sponsor = proposals.sponsor,
                        proposal.processor = proposals.processor,
                        proposal.sponsored = proposals.sponsored,
                        proposal.sponsoredAt = proposals.sponsoredAt,
                        proposal.processed = proposals.processed,
                        proposal.processedAt = proposals.processedAt,
                        proposal.didPass = proposals.didPass,
                        proposal.cancelled = proposals.cancelled,
                        proposal.cancelledAt = proposals.cancelledAt,
                        proposal.aborted = proposals.aborted,
                        proposal.executed = proposals.executed,
                        proposal.asOf = proposals.asOf,
                        proposal.ingestedBy = "{self.UPDATED_ID}"
                    return count(proposal)
            """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def create_or_merge_wallets(self, urls):
        return self.queries.create_wallets(urls)

    @count_query_logging
    def create_or_merge_tokens(self, urls):
        return self.queries.create_or_merge_tokens(urls, "ERC20")

    @count_query_logging
    def link_or_merge_votes(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS votes
                        MATCH (proposal:DaoHaus:Proposal {{id: votes.proposalId}}), (voter:Wallet {{address: toLower(votes.memberAddress)}})
                        WITH proposal, voter, votes
                        MERGE (voter)-[edge:VOTED]->(proposal)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.votedAt = votes.createdAt,
                            edge.uintVote = votes.uintVote,
                            edge.vote = votes.vote,
                            edge.shares = toInteger(votes.memberPower),
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
    def link_or_merge_voters(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS votes
                        MATCH (dao:DaoHaus:Dao {{id: votes.molochAddress}}), (voter:Wallet {{address: toLower(votes.memberAddress)}})
                        WITH dao, voter, votes
                        MERGE (voter)-[edge:IS_VOTER]->(dao)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.shares = toInteger(votes.memberPower),
                            edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.shares = toInteger(votes.memberPower),
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_summoners(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS summoners
                        MATCH (dao:DaoHaus:Dao {{id: summoners.daoId}}), (summoner:Wallet {{address: toLower(summoners.address)}})
                        WITH dao, summoner, summoners
                        MERGE (summoner)-[edge:SUMMONER]->(dao)
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
    def link_or_merge_sponsors(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS sponsors
                        MATCH (proposal:DaoHaus:Proposal {{id: sponsors.id}}), (sponsor:Wallet {{address: toLower(sponsors.sponsor)}})
                        WITH proposal, sponsor, sponsors
                        MERGE (sponsor)-[edge:SPONSORED]->(proposal)
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
    def link_or_merge_processors(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS processors
                        MATCH (proposal:DaoHaus:Proposal {{id: processors.id}}), (processor:Wallet {{address: toLower(processors.processor)}})
                        WITH proposal, processor, processors
                        MERGE (processor)-[edge:EXECUTED]->(proposal)
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
    def link_or_merge_proposers(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposers
                        MATCH (proposal:DaoHaus:Proposal {{id: proposers.id}}), (proposer:Wallet {{address: toLower(proposers.proposer)}})
                        WITH proposal, proposer, proposers
                        MERGE (proposer)-[edge:PROPOSED]->(proposal)
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
    def link_or_merge_applicants(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS applicants
                        MATCH (proposal:DaoHaus:Proposal {{id: applicants.id}}), (applicant:Wallet {{address: toLower(applicants.applicant)}})
                        WITH proposal, applicant, applicants
                        MERGE (applicant)-[edge:IS_APPLICANT]->(proposal)
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
    def link_or_merge_payments(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS payments
                        MATCH (proposal:DaoHaus:Proposal {{id: payments.id}}), (applicant:Wallet {{address: toLower(payments.applicant)}})
                        WITH proposal, applicant, payments
                        MERGE (proposal)-[edge:IS_PAYING]->(applicant)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.token = payments.paymentToken,
                            edge.amount = payments.paymentRequested,
                            edge.amountNumber = payments.payementAmount,
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
    def link_or_merge_tributes(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tributes
                        MATCH (proposal:DaoHaus:Proposal {{id: tributes.id}}), (applicant:Wallet {{address: toLower(tributes.applicant)}})
                        WITH proposal, applicant, tributes
                        MERGE (applicant)-[edge:IS_TRIBUTING]->(proposal)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.token = tributes.tributeToken,
                            edge.amount = tributes.tributeOffered,
                            edge.amountNumber = tributes.tributeAmount,
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
    def link_or_merge_members(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS members
                        MATCH (dao:DaoHaus:Dao {{id: members.molochAddress}}), (member:Wallet {{address: toLower(members.memberAddress)}})
                        WITH dao, member, members
                        MERGE (member)-[edge:IS_MEMBER]->(dao)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.tokenTribute = members.tokenTribute,
                            edge.shares = members.shares,
                            edge.loot = members.loot,
                            edge.exists = members.exists,
                            edge.didRagequit = members.didRagequit,
                            edge.kicked = members.kicked,
                            edge.jailed = members.jailed,
                            edge.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.ingestedBy = "{self.CREATED_ID}"
                        ON MATCH set edge.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            edge.tokenTribute = members.tokenTribute,
                            edge.shares = members.shares,
                            edge.loot = members.loot,
                            edge.exists = members.exists,
                            edge.didRagequit = members.didRagequit,
                            edge.kicked = members.kicked,
                            edge.jailed = members.jailed,
                            edge.ingestedBy = "{self.UPDATED_ID}"
                        return count(edge)
                """
            count += self.query(query)[0].value()
        return count

    @count_query_logging
    def link_or_merge_tokens(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MATCH (dao:DaoHaus:Dao {{id: tokens.daoId}}), (token:Token {{address: toLower(tokens.contractAddress)}})
                        WITH dao, token, tokens
                        MERGE (dao)-[edge:HAS_TOKEN]->(token)
                        ON CREATE set edge.uuid = apoc.create.uuid(),
                            edge.balance = tokens.balance,
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
    def link_or_merge_proposals(self, urls):
        count = 0
        for url in urls:
            query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MATCH (dao:DaoHaus:Dao {{id: proposals.molochAddress}}), (proposal:DaoHaus:Proposal {{id: proposals.id}})
                        WITH dao, proposal, proposals
                        MERGE (dao)-[edge:HAS_PROPOSAL]->(proposal)
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
    def clean_zero_address(self):
        query = """
            MATCH (wallet:Wallet {address: "0x0000000000000000000000000000000000000000"})
            DETACH DELETE wallet
        """