def merge_erc20_nodes(url, conn):

    erc20_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS erc20
                            MERGE (t:Transaction {{txHash: toLower(erc20.txHash)}})
                            ON CREATE set t:Transfer,
                                        t:Erc20,
                                        t.uuid = apoc.create.uuid(),
                                        t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                        t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                        t.tokenAddress = erc20.tokenAddress,
                                        t.amount = erc20.amount
                            return count(t)
                        """

    x = conn.query(erc20_node_query)
    print("erc20 nodes merged", x)


def merge_erc721_nodes(url, conn):

    erc20_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS erc
                            MERGE (t:Transaction {{txHash: toLower(erc.txHash)}})
                            ON CREATE set t:Transfer,
                                        t:Erc721,
                                        t.uuid = apoc.create.uuid(),
                                        t.createdDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                        t.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                        t.tokenAddress = erc.tokenAddress,
                                        t.tokenId = erc.tokenId
                            return count(t)
                        """

    x = conn.query(erc20_node_query)
    print("erc20 nodes merged", x)


def merge_sent_relationships(url, conn):

    sent_relationship_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' AS sent
                                MATCH (w:Wallet {{address: toLower(sent.address)}})
                                MERGE (t:Transaction {{txHash: toLower(sent.txHash)}})
                                MERGE (w)-[s:SENT]->(t)
                                return count(s)
                            """

    x = conn.query(sent_relationship_query)
    print("sent relationships merged", x)


def merge_sent_relationships(url, conn):

    sent_relationship_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' AS sent
                                MATCH (w:Wallet {{address: toLower(sent.address)}})
                                MERGE (t:Transaction {{txHash: toLower(sent.txHash)}})
                                MERGE (w)-[s:SENT]->(t)
                                return count(s)
                            """

    x = conn.query(sent_relationship_query)
    print("sent relationships merged", x)


def merge_received_relationships(url, conn):

    sent_relationship_query = f"""
                                LOAD CSV WITH HEADERS FROM '{url}' AS sent
                                MATCH (w:Wallet {{address: toLower(sent.address)}})
                                MERGE (t:Transaction {{txHash: toLower(sent.txHash)}})
                                MERGE (t)-[s:RECEIVED]->(w)
                                return count(s)
                            """

    x = conn.query(sent_relationship_query)
    print("received relationships merged", x)
