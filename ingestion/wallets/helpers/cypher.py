from datetime import datetime


def add_multisig_labels(url: str, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            MATCH (w:Wallet {{address: wallets.multisig}})
                            SET w:MultiSig,
                                w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                                w.threshold = toInteger(wallets.threshold)
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("MultiSig Labels Addeds", x)


def get_recent_non_categorized_wallets(cutoff: datetime, conn, split_size=20000):

    month = cutoff.month
    day = cutoff.day
    year = cutoff.year

    get_wallet_count_query = f"""
                                MATCH (w:Wallet) WHERE w.createdDt >= datetime({{year: {year}, month: {month}, day: {day}}}) 
                                AND NOT w:MultiSig AND NOT w:Eoa AND NOT w:Contract
                                RETURN count(w)
                            """

    x = conn.query(get_wallet_count_query)
    total = x[0].get("count(w)")

    all_wallets = []

    for i in range(0, total, split_size):
        print(i)
        get_wallets_query = f"""
                                MATCH (w:Wallet) WHERE w.createdDt >= datetime({{year: {year}, month: {month}, day: {day}}}) 
                                AND NOT w:MultiSig AND NOT w:Eoa AND NOT w:Contract
                                RETURN w.address
                                SKIP {i}
                                LIMIT {split_size}
                            """

        x = conn.query(get_wallets_query)
        all_wallets.extend([w.get("w.address") for w in x])

    print("Total wallets", len(all_wallets))

    return all_wallets


def get_recent_wallets(cutoff: datetime, conn, split_size=20000):

    month = cutoff.month
    day = cutoff.day
    year = cutoff.year

    get_wallet_count_query = f"""
                                    MATCH (w:Wallet) WHERE w.createdDt >= datetime({{year: {year}, month: {month}, day: {day}}}) 
                                    RETURN count(w)
                                """

    x = conn.query(get_wallet_count_query)
    total = x[0].get("count(w)")

    all_wallets = []

    for i in range(0, 200000, split_size):
        print(i)
        get_wallets_query = f"""
                                    MATCH (w:Wallet) WHERE w.createdDt >= datetime({{year: {year}, month: {month}, day: {day}}}) 
                                    RETURN w.address
                                    SKIP {i}
                                    LIMIT {split_size}
                                """

        x = conn.query(get_wallets_query)
        all_wallets.extend([w.get("w.address") for w in x])

    print("Total wallets", len(all_wallets))

    return all_wallets


def merge_signer_relationships(url: str, conn):

    signer_rel_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                        MATCH (m:Wallet {{address: wallets.multisig}}), (s:Wallet {{address: wallets.address}})
                        MERGE (s)-[r:IS_SIGNER]->(m)
                        return count(r)"""

    x = conn.query(signer_rel_query)
    print("signer relationships merged", x)


def add_eoa_labels(url, conn):

    wallet_node_query = f"""
                            LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                            MATCH (w:Wallet {{address: wallets.address}})
                            SET w:Eoa,
                                w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                            return count(w)
                        """

    x = conn.query(wallet_node_query)
    print("Eoa Labels Addeds", x)


def add_contract_labels(url: str, conn):
    wallet_node_query = f"""
                        LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                        MATCH (w:Wallet {{address: wallets.address}})
                        SET w:Contract,
                            w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms'))
                        return count(w)
                    """

    x = conn.query(wallet_node_query)
    print("Contract Labels Added", x)
