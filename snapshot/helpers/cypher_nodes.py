def create_unique_constraints(conn):

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    token_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
    conn.query(token_query)

    space_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Space) REQUIRE d.snapshotId IS UNIQUE"""
    conn.query(space_query)
    # it's fine if we have DAOhaus :Proposal and Snapshot :Proposal labels be the same index because the two ids are very different
    proposal_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:ProposalSnapshot) REQUIRE d.snapshotId IS UNIQUE"""
    conn.query(proposal_query)

    # we will most likely never search for a specific strategy so no need to index this
    # strategy_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Strategy) REQUIRE d.id IS UNIQUE"""
    # conn.query(strategy_query)

# removed the return count(*)
def create_wallet_nodes(url, conn):

    wallet_node_query = f"""
                            USING PERIODIC COMMIT 5000
                            LOAD CSV WITH HEADERS FROM '{url}' AS votes
                            MERGE (w:Wallet {{address: votes.voter}}) with w, votes
                            MATCH (p:Proposal {{snapshotId: votes.proposalId}}) 
                            WITH w, p, datetime(apoc.date.toISO8601(toInteger(votes.votedAt), 's')) AS vDt, votes.choice as choice
                            MERGE (w)-[:VOTED {{votedDt: vDt, choice: choice}}]->(p)
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print(x)
    print("wallet nodes created")


def create_token_nodes(url, conn):

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: tokens.address}})
                        ON CREATE set t = tokens
                        return count(t)
                    """

    x = conn.query(token_node_query)
    print(x)
    print("token nodes created")


def create_space_nodes(url, conn):

    space_node_query = f"""
                            USING PERIODIC COMMIT 5000
                            LOAD CSV WITH HEADERS FROM '{url}' AS spaces
                            MERGE(s:EntitySnapshotSpace:Snapshot:Space:Entity {{snapshotId: spaces.snapshotId}})
                            ON CREATE set s.uuid = apoc.create.uuid(),
                                s.name = spaces.name, 
                                s.chainId = toInteger(spaces.chainId), 
                                s.onlyMembers = spaces.onlyMembers, 
                                s.symbol = spaces.symbol,
                                s.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            ON MATCH set s.name = spaces.name,
                                s.onlyMembers = spaces.onlyMembers,
                                s.symbol = spaces.symbol,
                                s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(s)
                    """

    x = conn.query(space_node_query)
    print(x)
    print("space nodes created")

def create_proposal_nodes(url, conn):

    proposal_node_query = f"""
                        USING PERIODIC COMMIT 1000
                            LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                            MERGE (p:Snapshot:Proposal:ProposalSnapshot {{snapshotId: proposals.snapshotId}})
                            ON CREATE set p.uuid = apoc.create.uuid(),
                                p.snapshotId = proposals.snapshotId,
                                p.ipfsCID = proposals.ipfsCId,
                                p.title = proposals.title,
                                p.body = proposals.body,
                                p.choices = proposals.choices,
                                p.type = proposals.type,
                                p.author = proposals.author,
                                p.state = proposals.state,
                                p.link = proposals.link,
                                p.startDt = datetime(apoc.date.toISO8601(toInteger(proposals.startDt), 's')),
                                p.endDt = datetime(apoc.date.toISO8601(toInteger(proposals.endDt), 's')),
                                p.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            MERGE (w:Wallet {{address: proposals.author}})
                    """

    x = conn.query(proposal_node_query)
    print(x)
    print("proposal nodes created")

    proposal_rel_query = f"""
                        USING PERIODIC COMMIT 2000
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MATCH (p:ProposalSnapshot {{snapshotId: proposals.snapshotId}}), (w:Wallet {{address: proposals.author}}), (s:Space {{snapshotId: proposals.spaceId}})
                        MERGE (p)-[:HAS_AUTHOR]->(w)
                        MERGE (s)-[:HAS_PROPOSAL]->(p)"""

    x = conn.query(proposal_rel_query)
    print(x)
    print("proposal rels created")

# added datetime
def merge_proposal_nodes(url, conn):

    proposal_node_query = f"""
                                USING PERIODIC COMMIT 1000
                                LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                                MERGE(p:Snapshot:Proposal:ProposalSnapshot {{snapshotId: proposals.snapshotId}})
                                ON CREATE set p.uuid = apoc.create.uuid(),
                                  p.snapshotId = proposals.snapshotId,
                                  p.ipfsCID = proposals.ipfsCId,
                                  p.title = proposals.title,
                                  p.body = proposals.body,
                                  p.choices = proposals.choices,
                                  p.type = proposals.type,
                                  p.author = proposals.author,
                                  p.state = proposals.state,
                                  p.link = proposals.link,
                                  p.startDt = datetime(apoc.date.toISO8601(toInteger(proposals.startDt), 's')),
                                  p.endDt = datetime(apoc.date.toISO8601(toInteger(proposals.endDt), 's')),
                                  p.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                  p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                                ON MATCH set p.title = proposals.title,
                                  p.body = proposals.body,
                                  p.choices = proposals.choices,
                                  p.type = proposals.type,
                                  p.state = proposals.state,
                                  p.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                                MERGE (w:Wallet {{address: proposals.author}})
                                MERGE (p)-[:HAS_AUTHOR]->(w) WITH p, proposals
                                MATCH (s:EntitySnapshotSpace {{snapshotId: proposals.spaceId}}) with p, s
                                MERGE (s)-[:HAS_PROPOSAL]->(p)
                                return count(p)
                    """

    x = conn.query(proposal_node_query)
    print(x)
    print("proposal nodes created")


def create_strategy_nodes(url, conn):

    strategy_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                        MERGE(s:Snapshot:Strategy {{id: strategy.id}})
                        ON CREATE set s = strategy
                    """

    conn.query(strategy_node_query)
    print("strategy nodes created")
