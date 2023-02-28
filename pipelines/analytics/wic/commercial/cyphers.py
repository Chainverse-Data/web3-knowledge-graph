from datetime import datetime
import logging
from ...helpers import Cypher
from ...helpers import count_query_logging, get_query_logging


class LaborCyphers(Cypher):

    def __init__(self, database=None):
        super().__init__(database)
        self.conditions = [
            {
                "condition": "Paid Contributor",
                "contexts": ["DAOhaus - Paid Contributor", "Prop House - Auction Winner", "DAO Multisig Transfer Recipient"]
            },
                "condition": "DAO - Admin", 
                "contexts": "DAO Multisig Cosigner"
            {
                "condition": "Funding",
                "contexts": ["DAO Multisig - Funder"] ## it's not clear whether this is mul
            }
        self.subgraph_name = "Web3 Labor"

