def create_token_nodes(url, conn):

    unique_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""

    conn.query(unique_query)

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: tokens.address}})
                        ON CREATE set t = tokens
                    """

    conn.query(token_node_query)
    print("token nodes created")

def create_member_nodes(url, conn):

    # constraint was for the dao_member label when most of the queries below were looking for :Member, changed it to :Member label
    unique_query = """CREATE CONSTRAINT UniqueDAOAndWallet IF NOT EXISTS FOR (d:Member) REQUIRE (d.address, d.daoAddress) IS UNIQUE """
    # added constraint for wallet
    unique_query = """CREATE CONSTRAINT UniqueWallet IF NOT EXISTS FOR (d:Wallet) REQUIRE (d.address) IS UNIQUE """

    conn.query(unique_query)

    member_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS members
                        MERGE(t:Member:DaoHaus {{address: members.address, daoAddress: members.daoAddress}})
                        MERGE(w:Wallet {{address: members.address}})
                        ON CREATE set t = members
                    """
    conn.query(member_node_query)

    print("member nodes created")


def create_dao_nodes(url, conn):

    #changed from UniqueAddressId to UniqueDAOAddressId
    unique_query = """CREATE CONSTRAINT UniqueDAOAddressId IF NOT EXISTS FOR (d:Dao) REQUIRE d.address IS UNIQUE"""

    conn.query(unique_query)
    # removed unnecessary labels on our Merge statement
    dao_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS daos
                        MERGE(d:Dao:DaoHaus {{address: daos.address}})
                        ON CREATE SET d = daos,
                        d.createdAt = datetime(apoc.date.toISO8601(toInteger(daos.createdAt), 's')),
                        d.chain_id = toInteger(daos.chain_id),
                        d.totalShares = toInteger(daos.totalShares),
                        d.totalLoot = toInteger(daos.totalLoot),
                        d.periodDuration = toInteger(daos.periodDuration),
                        d.votingPeriodLength = toInteger(daos.votingPeriodLength),
                        d.dilutionBound = toInteger(daos.dilutionBound),
                        d.molochVersion = toInteger(daos.molochVersion)
                    """
    conn.query(dao_node_query)
    print("dao nodes created")


def create_proposal_nodes(url, conn):
    unique_query = """CREATE CONSTRAINT UniqueProposalId IF NOT EXISTS FOR (d:Proposal) REQUIRE d.id IS UNIQUE"""

    conn.query(unique_query)

    proposal_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MERGE(d:DaoHaus:Proposal {{id: proposals.id}})
                        ON CREATE set d = proposals,
                        d.createdAt = datetime(apoc.date.toISO8601(toInteger(proposals.createdAt), 's'))
                    """
    conn.query(proposal_node_query)
    print("proposal nodes created")

# Deposit token is missing from the dataset and so it has been removed
def create_dao_relationships(url, conn):
    # force use of :Member label
    relationship_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS row
                            MATCH (dao:Dao {{address: row.dao}}), (summoner:DaoHaus:Member {{address: row.summoner, daoAddress: row.dao}})
                            USING INDEX summoner:Member(address, daoAddress)
                            MERGE (summoner)-[:IS_SUMMONER]->(dao)
                            """
    conn.query(relationship_query)
    print("dao relationships created")

def create_approved_token_relationships(url, conn):
    relationship_query = f"""
                         LOAD CSV WITH HEADERS FROM '{url}' as row
                         MATCH (dao:Dao {{address: row.dao}}), (t:Token {{address: row.approvedToken}})
                         MERGE (dao)-[:HAS_APPROVED_TOKEN]->(t)
                        """

    conn.query(relationship_query)
    print("approved token relationships created")


def create_member_relationships(url, conn):
    relationship_query = f"""
                         USING PERIODIC COMMIT 1000
                         LOAD CSV WITH HEADERS FROM '{url}' as row
                         MATCH (d:Dao {{address: row.dao}}), (m:DaoHaus:Member {{address: row.member, daoAddress: row.dao}}), (w:Wallet {{address: row.member}})
                         USING INDEX m:Member(address, daoAddress)
                         MERGE (m)-[:IS_MEMBER]->(d)
                         MERGE (w)-[:IS_MEMBER]->(m)
                        """

    conn.query(relationship_query)
    print("member relationships created")

# seperated out into multiple queries because we have some null values for proposals not completed yet as well as empty addresses
# we could breat this out into one big query with nested FOREACH and CASE but that would be a bit harder to read and manager
def create_proposal_relationships(url, conn):
    rel_dao_proposal_query = f"""
                         USING PERIODIC COMMIT 1000
                         LOAD CSV WITH HEADERS FROM '{url}' as row
                         MATCH (d:Dao {{address: row.dao}}), (p:DaoHaus:Proposal {{id: row.proposal}})
                         MERGE (d)-[:HAS_PROPOSAL]->(p)
                        """

    rel_spon_prop_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' as row
                        MATCH (p:DaoHaus:Proposal {{id: row.proposal}}), (spon:DaoHaus:Member {{address: row.sponsor, daoAddress: row.dao}})
                        FOREACH(notblank IN CASE WHEN row.sponsor <>'0x0000000000000000000000000000000000000000' THEN [1] ELSE [] END | 
                        MERGE (spon)-[:SPONSORED]->(p))
                        """

    rel_prop_prop_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' as row
                        MATCH (p:DaoHaus:Proposal {{id: row.proposal}}), (prop:DaoHaus:Member {{address: row.proposer, daoAddress: row.dao}})
                        FOREACH(notblank IN CASE WHEN row.proposer <>'0x0000000000000000000000000000000000000000' THEN [1] ELSE [] END | 
                        MERGE (prop)-[:PROPOSED]->(p))
                        """

    rel_proc_prop_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' as row
                        MATCH (p:DaoHaus:Proposal {{id: row.proposal}}), (proc:DaoHaus:Member {{address: row.processor, daoAddress: row.dao}})
                        FOREACH(notblank IN CASE WHEN row.processor <>'0x0000000000000000000000000000000000000000' THEN [1] ELSE [] END | 
                        MERGE (proc)-[:PROCESSED]->(p))
                        """

    rel_app_prop_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' as row
                        MATCH (p:DaoHaus:Proposal {{id: row.proposal}}), (app:DaoHaus:Member {{address: row.applicant, daoAddress: row.dao}})
                        FOREACH(notblank IN CASE WHEN row.applicant <>'0x0000000000000000000000000000000000000000' THEN [1] ELSE [] END | 
                        MERGE (app)-[:IS_APPLICANT]->(p))
                        """

    conn.query(rel_dao_proposal_query)
    conn.query(rel_spon_prop_query)
    conn.query(rel_prop_prop_query)
    conn.query(rel_proc_prop_query)
    conn.query(rel_app_prop_query)
    print("proposal relationships created")


def create_vote_relationships(url, conn):
    relationship_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' as row
                        MATCH (p:DaoHaus:Proposal {{id: row.proposal}}), (voter:DaoHaus:Member {{address: row.member, daoAddress: row.dao}})
                        WITH datetime(apoc.date.toISO8601(toInteger(row.createdAt), 's')) AS cAt, p, voter, row
                        MERGE (voter)-[:VOTED {{createdAt: cAt, vote: row.vote}}]->(p)
                        """

    conn.query(relationship_query)
    print("vote relationships created")
