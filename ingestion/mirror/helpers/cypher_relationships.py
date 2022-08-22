def merge_author_relationships(url, conn):
    author_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as authors
                        MATCH (w:Wallet {{address: authors.contributor}}), (a:Mirror {{arweaveTx: authors.arweaveTx}})
                        MERGE (w)-[d:AUTHOR]->(p)
                        return count(d)
                    """

    x = conn.query(author_rel_query)
    print("author relationships created", x)


def merge_twitter_relationships(url, conn):
    twitter_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as twitters
                        MATCH (w:Twitter {{username: twitters.twitter}}), (a:Mirror {{arweaveTx: twitters.arweaveTx}})
                        MERGE (a)-[d:REFERENCES]->(w)
                        return count(d)
                    """

    x = conn.query(twitter_rel_query)
    print("twitter relationships created", x)
