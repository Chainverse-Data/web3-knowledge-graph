def merge_dao_nodes(url, conn):

    # removed unnecessary labels on our Merge statement
    dao_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS daos
                        MERGE (d:DaoHaus:Dao:Entity {{daohausId: daos.daohausId}})
                        ON CREATE SET d.uuid = apoc.create.uuid(),
                            d.name = daos.name,
                            d.occurDt = datetime(apoc.date.toISO8601(toInteger(daos.occurDt), 's')),
                            d.deleted = toBoolean(daos.deleted),
                            d.version = daos.version,
                            d.chain = daos.chain,
                            d.chainId = toInteger(daos.chainId),
                            d.totalShares = toInteger(daos.totalShares),
                            d.totalLoot = daos.totalLoot,
                            d.profileUrl = daos.profileUrl,
                            d.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET d.deleted = toBoolean(daos.deleted),
                            d.version = daos.version,
                            d.occurDt = datetime(apoc.date.toISO8601(toInteger(daos.occurDt), 's')),
                            d.totalShares = toInteger(daos.totalShares),
                            d.totalLoot = daos.totalLoot,
                            d.profileUrl = daos.profileUrl,
                            d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(d)
                    """

    x = conn.query(dao_node_query)
    print("dao nodes merged", x)


def merge_proposal_nodes(url, conn):

    proposal_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MERGE (d:DaoHaus:Proposal {{daohausId: proposals.daohausId}})
                        ON CREATE set d.uuid = apoc.create.uuid(),
                            d.text = proposals.text,
                            d.sharesRequested = toInteger(proposals.sharesRequested),
                            d.lootRequested = proposals.lootRequested,
                            d.tributeOffered = proposals.tributeOffered,
                            d.paymentRequested = proposals.paymentRequested,
                            d.yesShares = toInteger(proposals.yesShares),
                            d.noShares = toInteger(proposals.noShares),
                            d.processed = toBoolean(proposals.processed),
                            d.cancelled = toBoolean(proposals.sponsored),
                            d.startDt = datetime(apoc.date.toISO8601(toInteger(proposals.startDt), 's')),
                            d.endDt = datetime(apoc.date.toISO8601(toInteger(proposals.endDt), 's')),
                            d.occurDt = datetime(apoc.date.toISO8601(toInteger(proposals.occurDt), 's')),
                            d.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH set d.text = proposals.text,
                            d.sharesRequested = toInteger(proposals.sharesRequested),
                            d.lootRequested = proposals.lootRequested,
                            d.tributeOffered = proposals.tributeOffered,
                            d.paymentRequested = proposals.paymentRequested,
                            d.yesShares = toInteger(proposals.yesShares),
                            d.noShares = toInteger(proposals.noShares),
                            d.processed = toBoolean(proposals.processed),
                            d.cancelled = toBoolean(proposals.sponsored),
                            d.startDt = datetime(apoc.date.toISO8601(toInteger(proposals.startDt), 's')),
                            d.endDt = datetime(apoc.date.toISO8601(toInteger(proposals.endDt), 's')),
                            d.occurDt = datetime(apoc.date.toISO8601(toInteger(proposals.occurDt), 's')),
                            d.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(d)
                    """
    x = conn.query(proposal_node_query)
    print("proposal nodes merged", x)
