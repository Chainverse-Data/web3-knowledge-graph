def create_proposal_relationships(url, conn):
    # fixed typo was {id: proposals.id when it should have been {id: proposals.proposals}
    proposal_rel_query = f"""
                        USING PERIODIC COMMIT 1000
                        LOAD CSV WITH HEADERS FROM '{url}' AS proposals
                        MATCH (p:Snapshot:Proposal {{id: proposals.proposal}}), (author:Wallet {{address: proposals.author}}), (space:Snapshot:Space {{id: proposals.space}})
                        MERGE (author)-[:AUTHORED]->(p)
                        MERGE (space)-[:HAS_PROPOSAL]->(p)
                    """

    conn.query(proposal_rel_query)
    print("proposal relaionships created")


def create_vote_relationships(url, conn):

    vote_rel_query = f"""
                    USING PERIODIC COMMIT 10000
                    LOAD CSV WITH HEADERS FROM '{url}' as votes
                    MATCH (voter:Wallet {{address: votes.voter}}), (p:Proposal {{id: votes.proposal}})
                    WITH voter, p, datetime(apoc.date.toISO8601(toInteger(votes.createdAt), 's')) AS cAt, votes
                    MERGE (voter)-[:VOTED {{ipfs: votes.ipfs, createdAt: cAt, choice: votes.choice}}]->(p)
                    """

    conn.query(vote_rel_query)
    print("vote relationships created")

def create_strategy_relationships(url, conn):

    strat_rel_query = f"""                       
                        USING PERIODIC COMMIT 5000
                        LOAD CSV WITH HEADERS FROM '{url}' as strats
                        MATCH (t:Token {{address: strats.token}}), (s:Space {{id: strats.space}})
                        MERGE (s)-[n:HAS_STRATEGY]->(t)
                        return count(n)
                    """

    x = conn.query(strat_rel_query)
    print(x)
    print("strategy relationships created")
