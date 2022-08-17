def create_unique_constraints(conn):

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    token_query = """CREATE CONSTRAINT UniqueTokenAddress IF NOT EXISTS FOR (d:Token) REQUIRE d.address IS UNIQUE"""
    conn.query(token_query)

    space_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Space) REQUIRE d.snapshotId IS UNIQUE"""
    conn.query(space_query)
    # it's fine if we have DAOhaus :Proposal and Snapshot :Proposal labels be the same index because the two ids are very different
    proposal_query = """CREATE CONSTRAINT UniqueID IF NOT EXISTS FOR (d:Proposal) REQUIRE d.snapshotId IS UNIQUE"""
    conn.query(proposal_query)


def create_indexes(conn):
    proposal_query = """CREATE INDEX UniquePropID IF NOT EXISTS FOR (n:Proposal) ON (n.snapshotId)"""
    conn.query(proposal_query)

    space_query = """CREATE INDEX UniqueSpaceID IF NOT EXISTS FOR (n:Space) ON (n.snapshotId)"""
    conn.query(space_query)

    wallet_query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
    conn.query(wallet_query)


def create_wallet_nodes(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            CREATE (w:Wallet {{address: wallets.address}})
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("wallet nodes created", x)


def merge_wallet_nodes(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            MERGE (w:Wallet {{address: wallets.address}})
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("wallet nodes merged", x)


def create_token_nodes(url, conn):

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        CREATE (t:Token {{address: tokens.address}})
                        set t = tokens
                        return count(t)
                    """

    x = conn.query(token_node_query)
    print("token nodes created", x)


def merge_token_nodes(url, conn):

    token_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS tokens
                        MERGE(t:Token {{address: tokens.address}})
                        ON CREATE set t = tokens
                    """

    x = conn.query(token_node_query)
    print("token nodes merged", x)


def create_space_nodes(url, conn):
    space_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS spaces
                            CREATE (s:EntitySnapshotSpace:Snapshot:Space:Entity {{snapshotId: spaces.snapshotId}})
                            set s.uuid = apoc.create.uuid(),
                                s.name = spaces.name, 
                                s.chainId = toInteger(spaces.chainId), 
                                s.onlyMembers = spaces.onlyMembers, 
                                s.symbol = spaces.symbol,
                                s.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(s)
                    """

    x = conn.query(space_node_query)
    print("space nodes created", x)


def merge_space_nodes(url, conn):

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
    print("space nodes merged", x)


def create_proposal_nodes(url, conn):
    proposal_node_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                                CREATE(p:Snapshot:Proposal:ProposalSnapshot {{snapshotId: proposals.snapshotId}})
                                set p.uuid = apoc.create.uuid(),
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
                                return count(p)
                            """

    x = conn.query(proposal_node_query)
    print("proposal nodes created", x)


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
                                return count(p)
                    """

    x = conn.query(proposal_node_query)
    print("proposal nodes merged", x)


def merge_strategy_nodes(url, conn):

    strategy_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                        MERGE(s:Snapshot:Strategy {{id: strategy.id}})
                        ON CREATE set s = strategy
                    """

    x = conn.query(strategy_node_query)
    print("strategy nodes merged", x)


def create_strategy_nodes(url, conn):

    strategy_node_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                        CREATE (s:Snapshot:Strategy {{id: strategy.id}})
                        set s = strategy
                    """

    x = conn.query(strategy_node_query)
    print("strategy nodes created", x)
