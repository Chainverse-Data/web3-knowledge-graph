def merge_member_relationships(url, conn):

    member_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as members
                        MATCH (w:Wallet {{address: toLower(members.address)}}), (s:DaoHaus {{daohausId: members.daoId}})
                        MERGE (w)-[r:CONTRIBUTOR]->(s)
                        ON CREATE SET r.type = 'member',
                            r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            r.occurDt = datetime(apoc.date.toISO8601(toInteger(members.occurDt), 's')),
                            r.shares = toInteger(members.shares),
                            r.loot = toInteger(members.loot),
                            r.kicked = toBoolean(members.kicked),
                            r.jailed = toBoolean(members.jailed)
                        ON MATCH SET  r.shares = toInteger(members.shares),
                            r.loot = toInteger(members.loot),
                            r.kicked = toBoolean(members.kicked),
                            r.jailed = toBoolean(members.jailed),
                            r.occurDt = datetime(apoc.date.toISO8601(toInteger(members.occurDt), 's')),
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(r)
                        """

    x = conn.query(member_rel_query)
    print("member relationships created", x)


def merge_token_dao_relationships(url, conn):

    token_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as tokens
                        MATCH (t:Token {{address: toLower(tokens.address)}}), (s:DaoHaus {{daohausId: tokens.daoId}})
                        MERGE (s)-[n:HAS_TOKEN]->(t)
                        return count(n)
    """

    x = conn.query(token_rel_query)
    print("token relationships created", x)


def merge_proposal_author_relationships(url, conn):

    author_sponsor_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as authors
                        MATCH (w:Wallet {{address: toLower(authors.author)}}), (p:DaoHaus {{daohausId: authors.daohausId}})
                        MERGE (w)-[r:AUTHOR]->(p)
                        return count(r)
                        """

    x = conn.query(author_sponsor_rel_query)
    print("proposal author relationships created", x)


def merge_proposal_sponsor_relationships(url, conn):

    author_sponsor_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as authors
                        MATCH (s:Wallet {{address: toLower(authors.sponsor)}}), (p:DaoHaus {{daohausId: authors.daohausId}})
                        MERGE (s)-[x:SPONSOR]->(p)
                        return count(x)
                        """

    x = conn.query(author_sponsor_rel_query)
    print("proposal sponsor relationships created", x)


def merge_proposal_dao_relationships(url, conn):

    proposal_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as proposals
                        MATCH (d:DaoHaus {{daohausId: proposals.daoId}}), (p:DaoHaus {{daohausId: proposals.daohausId}})
                        MERGE (d)-[n:HAS_PROPOSAL]->(p)
                        return count(n)
    """

    x = conn.query(proposal_rel_query)
    print("proposal dao relationships created", x)


def merge_vote_relationships(url, conn):

    vote_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as votes
                        MATCH (w:Wallet {{address: toLower(votes.address)}}), (p:DaoHaus {{daohausId: votes.proposalId}})
                        MERGE (w)-[r:VOTED]->(p)
                        ON CREATE SET r.choice = votes.choice,
                            r.votedDt = datetime(apoc.date.toISO8601(toInteger(votes.votedDt), 's')),
                            r.occurDt = datetime(apoc.date.toISO8601(toInteger(votes.votedDt), 's'))
                        ON MATCH SET r.choice = votes.choice,
                            r.votedDt = datetime(apoc.date.toISO8601(toInteger(votes.votedDt), 's')),
                            r.occurDt = datetime(apoc.date.toISO8601(toInteger(votes.votedDt), 's'))
                        return count(r)
                    """

    x = conn.query(vote_rel_query)
    print("vote relationships created", x)


def merge_summoner_relationships(url, conn):

    summoner_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as summoners
                        MATCH (w:Wallet {{address: toLower(summoners.summoner)}}), (s:DaoHaus {{daohausId: summoners.daohausId}})
                        MERGE (w)-[r:SUMMONER]->(s)
                        return count(r)
                        """

    x = conn.query(summoner_rel_query)
    print("summoner relationships created", x)
