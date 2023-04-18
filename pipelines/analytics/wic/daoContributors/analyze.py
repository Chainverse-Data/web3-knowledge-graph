from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import DaoCyphers
import pandas as pd
import logging

class DaoAnalysis(WICAnalysis):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.subgraph_name = "DaoContributors"
        self.conditions = {
           "DaoContributors": {
                "SnapshotAdmin": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD",
                        "call": self.process_org_snapshot_contributors
                    },
                "MultisigSigner": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD",
                        "call": self.process_org_multisig_signers
                    },
                "DaoFundingRecipient": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD",
                        "call": self.process_dao_funding_recipient
                    },
                "DaoTreasuryFunder": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_dao_treasury_funder
                },
                "TechnicalContributor": {
                    "types": [TYPES['experiences']], 
                    "definition": "TBD", 
                    "call": self.process_technical_contributor
                }
           }
        }
        self.cyphers = DaoCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-dao-contributors")

    def process_org_multisig_signers(self, context):
        logging.info("Identifying signers of organization multisigs...")
        self.cyphers.get_org_multisig_signers(context)

    def process_org_snapshot_contributors(self, context):
        logging.info("Identifying snapshot contributors...")
        self.cyphers.get_snapshot_contributors(context)

    def process_dao_treasury_funder(self, context):
        logging.info("DAO treasury funders...")
        self.cyphers.get_dao_treasury_funders(context)

    def process_dao_funding_recipient(self, context):
        logging.info("DAO funding recipients....")
        self.cyphers.get_dao_funding_recipients(context)

    def process_technical_contributor(self, context):
        logging.info('get technical contributors...')
        self.cyphers.get_technical_contributors(context)

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = DaoAnalysis()
    analysis.run()
