from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import DevelopersCyphers

class DevelopersAnalysis(WICAnalysis):
    """This class builds the seed for the developer subgraph in Neo4j"""
    def __init__(self):
        self.conditions = {
            "Web3Developers": {
                "GithubAccount": 
                    {
                        "type": TYPES["interests"],
                        "definition": "This wallet has a GitHub account.",
                        "call": self.process_dev_accounts
                    },
                "DuneAccount": {
                    "type": TYPES['interests'],
                    "definition": "blah",
                    "call": self.process_dune_accounts
                },
                "GitcoinBountyFulfill":
                    {
                        "type": TYPES["experiences"],
                        "definition": "TBD",
                        "call": self.process_bounty_fullfilers
                    },
                "SolidityDeveloper": {
                    "call": self.process_solidity_devs,
                    "definition": "TBD",
                    "type": TYPES["experiences"]
                }
            },
            "TechnicalEcosystemDevelopment": {
                "GitcoinBountyAdmin": 
                    {
                        "call": self.process_gitcoin_bounty_admin,
                        "definition": "TBD",
                        "type": TYPES["experiences"]
                    }
            }
        }
        self.subgraph_name = "Developers"
        self.cyphers = DevelopersCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-developers")

    def process_solidity_devs(self, context):
        self.cyphers.is_solidity_developer(context)

    def process_dev_accounts(self, context):
        self.cyphers.has_github(context)

    def process_bounty_fullfilers(self, context):
        self.cyphers.gitcoin_bounty_fulfill(context)
    
    def process_gitcoin_bounty_admin(self, context):
        self.cyphers.gitcoin_bounty_admin(context)

    def process_dune_accounts(self, context):
        self.cyphers.identify_dune_accounts(context)

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = DevelopersAnalysis()
    analysis.run()
