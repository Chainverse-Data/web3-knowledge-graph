from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import DevelopersCyphers

class DevelopersAnalysis(WICAnalysis):
    """This class builds the seed for the developer subgraph in Neo4j"""
    def __init__(self):
        self.conditions = {
            "DevContributor": {
                "DevAccount": 
                    {
                        "type": TYPES["interests"],
                        "call": self.process_dev_accounts
                    },
                "GitcoinBountyFulfill":
                    {
                        "type": TYPES["experiences"],
                        "call": self.process_bounty_fullfilers
                    },
                "SolidityDeveloper": {
                    "call": self.process_solidity_devs,
                    "type": TYPES["experiences"]
                }
            },
            "DevEcosystem": {
                "GitcoinBountyAdmin": 
                    {
                        "call": self.process_gitcoin_bounty_admin,
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

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = DevelopersAnalysis()
    analysis.run()
