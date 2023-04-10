from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import ProtocolPoliticiansCyphers

class ProtocolPoliticiansAnalysis(WICAnalysis):

    def __init__(self):
        self.conditions = {
            "Voting": {
                "EngagedVoter": {
                    "types": [TYPES["experiences"]],
                    "definition": "TBD",
                    "call": self.process_engaged_voters
                    }    
            },
            "Proposals": {
                "ProposalAuthor": {
                    "types": [TYPES["experiences"]],
                    "definition": "TBD",
                    "call": self.process_proposal_authors
                    }
            }, 
            "Delegation": {
                "Delegate": {
                    "types": [TYPES["experiences"]],
                    "definition": "TBD",
                    "call": self.process_delegates
                    }
            },
            "Leadership": {
                "DaoAdmin": {
                    "types": [TYPES["experiences"]],
                    "definition": "TBD",
                    "call": self.process_dao_admins
                    }
            }
        }
        self.subgraph_name = "ProtocolGovernance"

        self.cyphers = ProtocolPoliticiansCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-protocol-politicians")

    def process_engaged_voters(self, context):
        self.cyphers.connect_voters(context)

    def process_proposal_authors(self, context):
        benchmark = self.cyphers.get_proposal_authors_benchmark()
        self.cyphers.connect_proposal_author(context, benchmark)

    def process_delegates(self, context):
        self.cyphers.connect_delegates(context)

    def process_dao_admins(self, context):
        self.cyphers.connect_dao_admins(context)

    def run(self):
        self.process_conditions()

if __name__=="__main__":
    analysis = ProtocolPoliticiansAnalysis()
    analysis.run()
