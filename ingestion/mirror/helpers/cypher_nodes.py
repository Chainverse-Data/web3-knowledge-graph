def create_unique_constraints(conn):

    wallet_query = """CREATE CONSTRAINT UniqueAddress IF NOT EXISTS FOR (w:Wallet) REQUIRE w.address IS UNIQUE"""
    conn.query(wallet_query)

    article_query = """CREATE CONSTRAINT UniqueArticleID IF NOT EXISTS FOR (a:Mirror) REQUIRE a.arweaveTx IS UNIQUE"""
    conn.query(article_query)


def create_indexes(conn):

    wallet_query = """CREATE INDEX UniqueAddress IF NOT EXISTS FOR (n:Wallet) ON (n.address)"""
    conn.query(wallet_query)

    article_query = """CREATE INDEX UniqueArticleID IF NOT EXISTS FOR (n:Mirror) ON (n.arweaveTx)"""
    conn.query(article_query)

    twitter_query = """CREATE INDEX UniqueTwitterID IF NOT EXISTS FOR (n:Twitter) ON (n.username)"""
    conn.query(twitter_query)


def create_wallet_nodes(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            CREATE (w:Wallet {{address: wallets.address}})
                            SET w.uuid = apoc.create.uuid()
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("wallet nodes created", x)


def merge_wallet_nodes(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            MERGE (w:Wallet {{address: wallets.address}})
                            ON CREATE set w.uuid = apoc.create.uuid()
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("wallet nodes merged", x)

def merge_twitter_nodes(url, conn):

    twitter_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS twitters
                            MERGE (w:Account:Twitter {{username: twitters.twitter}})
                            ON CREATE set w.uuid = apoc.create.uuid()
                            return count(w)
                        """
    
    x = conn.query(twitter_node_query)
    print("twitter nodes merged", x)


def merge_article_nodes(url, conn):
    article_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS articles
                            MERGE (a:Mirror:Article {{arweaveTx: articles.arweaveTx}})
                            ON CREATE set a.uuid = apoc.create.uuid(),
                            a.title = articles.title,
                            a.body = articles.body,
                            a.datePublished = datetime(apoc.date.toISO8601(toInteger(articles.datePublished), 's')),
                            a.contributor = articles.contributor,
                            a.publication = articles.publication
                            ON MATCH set a.title = articles.title,
                            a.body = articles.body,
                            a.datePublished = datetime(apoc.date.toISO8601(toInteger(articles.datePublished), 's')),
                            a.contributor = articles.contributor,
                            a.publication = articles.publication
                            return count(a)
                        """

    x = conn.query(article_node_query)
    print("article nodes merged", x)
