def merge_proposal_space_relationships(url, conn):

    proposal_space_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as proposals
                        MATCH (p:Proposal {{snapshotId: proposals.snapshotId}}), (s:Space {{snapshotId: proposals.spaceId}})
                        MERGE (s)-[d:HAS_PROPOSAL]->(p)
                        return count(d)
                    """

    x = conn.query(proposal_space_rel_query)
    print("proposal space relationships created", x)


def merge_proposal_author_relationships(url, conn):

    proposal_author_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as proposals
                        MATCH (p:Proposal {{snapshotId: proposals.snapshotId}}), (w:Wallet {{address: proposals.author}})
                        MERGE (p)-[d:HAS_AUTHOR]->(w)
                        return count(d)
                    """

    x = conn.query(proposal_author_rel_query)
    print("proposal author relationships created", x)


def merge_vote_relationships(url, conn):

    vote_rel_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS votes
                            MATCH (w:Wallet {{address: votes.voter}}), (p:Proposal {{snapshotId: votes.proposalId}}) 
                            WITH w, p, datetime(apoc.date.toISO8601(toInteger(votes.votedAt), 's')) AS vDt, votes.choice as choice
                            MERGE (w)-[v:VOTED]->(p)
                            ON CREATE set v.votedDt = vDt,
                                v.choice = choice
                            ON MATCH set v.votedDt = vDt,
                                v.choice = choice
                            return count(v)
                        """

    x = conn.query(vote_rel_query)
    print("vote relationships created", x)


def merge_strategy_relationships(url, conn):

    strat_rel_query = f"""                       
                        LOAD CSV WITH HEADERS FROM '{url}' as strats
                        MATCH (t:Token {{address: strats.token}}), (s:Space {{snapshotId: strats.space}})
                        MERGE (s)-[n:HAS_STRATEGY]->(t)
                        return count(n)
                    """

    x = conn.query(strat_rel_query)
    print("strategy relationships created", x)


def merge_space_alias_relationships(url, conn):

    ens_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as ens
                        MATCH (s:Space {{snapshotId: ens.name}}),
                            (a:Alias {{name: toLower(ens.name)}})
                        MERGE (s)-[n:HAS_ALIAS]->(a)
                        return count(n)
                    """

    x = conn.query(ens_rel_query)
    print("ens relationships created", x)


def merge_member_relationships(url, conn):

    member_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as members
                        MATCH (w:Wallet {{address: toLower(members.address)}}), (s:Space {{snapshotId: members.space}})
                        MERGE (w)-[r:CONTRIBUTOR]->(s)
                        ON CREATE SET r.type = 'member',
                            r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET r.type = 'member'
                        return count(r)
                        """

    x = conn.query(member_rel_query)
    print("member relationships created", x)


def merge_admin_relationships(url, conn):

    admin_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as admins
                        MATCH (w:Wallet {{address: toLower(admins.address)}}), (s:Space {{snapshotId: admins.space}})
                        MERGE (w)-[r:CONTRIBUTOR]->(s)
                        ON CREATE SET r.type = 'admin',
                            r.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                            r.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        ON MATCH SET r.type = 'admin'
                        return count(r)
                        """

    x = conn.query(admin_rel_query)
    print("admin relationships created", x)


def merge_twitter_relationships(url, conn):

    twitter_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as twitter
                        MATCH (s:Space {{snapshotId: twitter.snapshotId}}), (t:Twitter {{handle: toLower(twitter.handle)}})
                        MERGE (s)-[r:HAS_ACCOUNT]->(t)
                        return count(r)"""

    x = conn.query(twitter_rel_query)
    print("twitter relationships created", x)
