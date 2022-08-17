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
                            MERGE (w)-[:VOTED {{votedDt: vDt, choice: choice}}]->(p)
                            return count(w)
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
