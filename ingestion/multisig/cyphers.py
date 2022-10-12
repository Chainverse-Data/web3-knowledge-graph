from ..helpers import Cypher
import logging
import sys


class MultisigCyphers(Cypher):
    def __init__(self):
        super().__init__()

    def create_custom_constraints(self):
        pass

    def create_custom_indexes(self):
        pass

    def add_multisig_labels(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (w:Wallet {{address: wallets.multisig}})
                    SET w:MultiSig,
                        w.lastUpdateDt = datetime(apoc.date.toISO8601(apoc.date.currentTimestamp(), 'ms')),
                        w.threshold = toInteger(wallets.threshold),
                        w.occurDt = datetime(apoc.date.toISO8601(toInteger(wallets.occurDt), 'ms'))
                    return count(w)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count

    def link_multisig_signer(self, urls):
        logging.info(f"Ingesting with: {sys._getframe().f_code.co_name}")
        count = 0
        for url in urls:
            query = f"""
                    LOAD CSV WITH HEADERS FROM '{url}' AS wallets
                    MATCH (m:Wallet {{address: wallets.multisig}}), (s:Wallet {{address: wallets.address}})
                    MERGE (s)-[r:IS_SIGNER]->(m)
                    return count(r)
            """
            count += self.query(query)[0].value()
        logging.info(f"Created or merged: {count}")
        return count
