def create_unique_constraints(conn):

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    article_query = """CREATE CONSTRAINT UniqueArticleID IF NOT EXISTS FOR (a:Mirror) REQUIRE a.arweaveTx IS UNIQUE"""
    conn.query(article_query)


def merge_article_nodes(url, conn):
    article_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS articles
                            MERGE (a:Mirror:Article {{uri: articles.arweaveTx}})
                            ON CREATE set a.uuid = apoc.create.uuid(),
                                a.title = articles.title,
                                a.text = articles.text,
                                a.datePublished = datetime(apoc.date.toISO8601(toInteger(articles.datePublished), 's')),
                                a.contributor = articles.contributor,
                                a.publication = articles.publication,
                                a.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))

                            ON MATCH set a.title = articles.title,
                                a.body = articles.body,
                                a.datePublished = datetime(apoc.date.toISO8601(toInteger(articles.datePublished), 's')),
                                a.contributor = articles.contributor,
                                a.publication = articles.publication,
                                a.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(a)
                        """

    x = conn.query(article_node_query)
    print("article nodes merged", x)
