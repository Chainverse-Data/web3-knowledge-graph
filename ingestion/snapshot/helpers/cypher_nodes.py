def merge_space_nodes(url, conn):

    space_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS spaces
                            MERGE(s:EntitySnapshotSpace:Snapshot:Space:Entity {{snapshotId: spaces.snapshotId}})
                            ON CREATE set s.uuid = apoc.create.uuid(),
                                s.name = spaces.name, 
                                s.chainId = toInteger(spaces.chainId), 
                                s.onlyMembers = spaces.onlyMembers, 
                                s.symbol = spaces.symbol,
                                s.twitter = spaces.twitter,
                                s.profileUrl = spaces.profileUrl,
                                s.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            ON MATCH set s.name = spaces.name,
                                s.onlyMembers = spaces.onlyMembers,
                                s.twitter = spaces.twitter,
                                s.symbol = spaces.symbol,
                                s.profileUrl = spaces.profileUrl,
                                s.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(s)
                    """

    x = conn.query(space_node_query)
    print("space nodes merged", x)


def merge_proposal_nodes(url, conn):

    proposal_node_query = f"""
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
                        LOAD CSV WITH HEADERS FROM '{url}' AS strategy
                        MERGE(s:Snapshot:Strategy {{id: strategy.id}})
                        ON CREATE set s = strategy
                    """

    x = conn.query(strategy_node_query)
    print("strategy nodes merged", x)

