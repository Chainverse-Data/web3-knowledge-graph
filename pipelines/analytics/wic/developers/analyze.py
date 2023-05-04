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
                        "types": [TYPES["interests"]],
                        "definition": "This wallet has a GitHub account.",
                        "weight": 0,
                        "call": self.process_dev_accounts
                    },
                "DuneAccount": {
                    "types": [TYPES['interests']],
                    "definition": "blah",
                    "weight": 0,
                    "call": self.process_dune_accounts
                },
                "GitcoinBountyFulfill":
                    {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD", 
                        "weight": 0,
                        "call": self.process_bounty_fullfilers
                    },
                "SmartContractDev": 
                    {
                        "call": self.process_smart_contract_dev,
                        "definition": "TBD", 
                        "weight": 0,
                        "types": [TYPES["experiences"]]
                    }
            },
            "TechnicalEcosystemDevelopment": {
                "GitcoinBountyAdmin": 
                    {
                        "call": self.process_gitcoin_bounty_admin,
                        "definition": "TBD", 
                        "weight": 0,
                        "types": [TYPES["experiences"]]
                    }
            }
        }
        self.subgraph_name = "Developers"
        self.cyphers = DevelopersCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-developers")

    def process_smart_contract_dev(self, context):
        self.cyphers.is_smart_contract_dev(context)

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
