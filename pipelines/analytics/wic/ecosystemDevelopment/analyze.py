from .. import WICAnalysis
from .cyphers import EcoDevCyphers

class EcoDevAnalysis(WICAnalysis):
    """This class builds out the ecosystem development wallet in context (Wic) subgraph"""
    def __init__(self):
        self.subgraph_name = 'EcoDev'
        self.conditions = {
            "Grants": {
                "GitcoinGrantAdmin": self.process_gitcoin_grant_admins, 
                "GitcoinGrantDonor": self.process_gitcoin_grant_donor,
                "GrantsDao": self.process_grants_dao
            },
            "Bounties": {
                "GitcoinBountyAdmin": self.process_gitcoin_bounty_creators,
                "GitcoinBountyFulfill": self.process_gitcoin_bounty_fulfillers, 
            },
            "Incubators": {
                "Incubator": {
                    "call": self.process_incubator, 
                    "subcontexts": {
                        "IncubatorMember": self.process_incubator_member, 
                        "IncubatorParticipant": self.process_incubator_participant
                    }
                }
            } 
        }
        self.cyphers = EcoDevCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-ecodev")

        # This will need to change to a more automatic method.
        self.gdaos = ['Metacartel', 'Unlock Protocol', 'MetaGammaDelta', 'Grants', 'Gitcoin', 'Unlock Protocol']
        self.incubators = ['Seed Club']

    def process_gitcoin_grant_admins(self, context):
        benchmark = self.cyphers.get_grant_admin_benchmark()
        self.cyphers.connect_gitcoin_grant_admins(context, benchmark)

    def process_gitcoin_grant_donor(self, context):
        benchmark = self.cyphers.get_grant_donation_benchmark()
        self.cyphers.connect_gitcoin_grant_donors(context, benchmark)

    def process_grants_dao(self, context):
        self.cyphers.connect_grants_daos(context, self.gdaos)
        self.cyphers.connect_grant_dao_wallets(context)

    def process_gitcoin_bounty_creators(self, context):
        benchmark = self.cyphers.get_gitcoin_bounty_creator_benchmark()
        self.cyphers.connect_gitcoin_bounty_creators(context, benchmark)

    def process_gitcoin_bounty_fulfillers(self, context):
        benchmark = self.cyphers.get_gitcoin_bounty_fullfilers_benchmark()
        self.cyphers.connect_gitcoin_bounty_fulfillers(context, benchmark)

    def process_incubator(self, context):
        self.cyphers.connect_incubators(context, self.incubators)

    def process_incubator_member(self, root_context, context):
        self.cyphers.connect_incubators_members(root_context, context)

    def process_incubator_participant(self, root_context, context):
        self.cyphers.connect_incubators_participant(root_context, context)

    def run(self):
        self.process_conditions()

if __name__=='__main__':
    analysis = EcoDevAnalysis()
    analysis.run()
