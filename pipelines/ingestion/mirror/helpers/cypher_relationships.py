def merge_author_relationships(url, conn):
    author_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as authors
                        MATCH (w:Wallet {{address: authors.contributor}}), (a:Mirror:Article {{uri: authors.arweaveTx}})
                        MERGE (w)-[d:AUTHOR]->(a)
                        return count(d)
                    """

    x = conn.query(author_rel_query)
    print("author relationships created", x)


def merge_twitter_relationships(url, conn):
    twitter_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' as twitters
                        MATCH (w:Twitter {{handle: twitters.handle}}), (a:Mirror {{uri: twitters.arweaveTx}})
                        MERGE (a)-[d:REFERENCES]->(w)
                        return count(d)
                    """

    x = conn.query(twitter_rel_query)
    print("twitter relationships created", x)
