
from .. import WICAnalysis
from .cyphers import ProfessionalsCyphers
import logging

class ProfessionalsAnalysis(WICAnalysis):
    """Identitifes people who "work" in Web3"""
    def __init__(self):

        
        self.conditions = {
            "DaoContributors": {
                "DaoTokenContractDeployers": self.process_token_contract_deployer_wallets,
                "CommunityWalletDeployers": self.process_org_wallet_deployers,
                "SnapshotContributors": self.process_org_snapshot_contributors,
                "CommunityMultisigSigners": self.process_org_multisig_signers,
                "OrgEnsCustodian": self.proces
            },
            "Web3Professionals": {
                "Founders": self.process_founder_bios,
                "Investors": self.process_investor_bios,
            },
            "Influencers": {
                "TwitterInfluencers": self.process_twitter_influencers,
                "Podcasters": self.process_podcaster_bios

            }
        }
        self.subgraph_name = "Professionals"
        self.cyphers = ProfessionalsCyphers(self.subgraph_name, self.conditions)

        super().__init__("wic-professionals")

    def process_token_contract_deployer_wallets(self, context):
        logging.info("Identifying token contract deployers")
        self.cyphers.token_contract_deployer_wallets(context)

    def process_org_wallet_deployers(self, context):
        logging.info("Identifying DAO treasury & ops wallet deployers...")
        self.cyphers.identify_org_wallet_deployers(context)
    
    def process_org_multisig_signers(self, context):
        logging.info("Identifying signers of organization multisigs...")
        self.cyphers.get_org_multisig_signers(self, context)
    
    def process_org_snapshot_contributors(self, context):
        logging.info("Identifying snapshot contributors...")
        self.cyphers.get_snapshot_contributors(self, context)

    def process_org_ens_admin(self, context):
        logging.info("Identifying ENS administrators...")
        self.cyphers.get_ens_admin(self, context)

    def process_founder_bios(self, context):
        logging.info("Identifying founders based on bios..")
        self.cyphers.identify_founders_bios(self, context)

    def process_company_officer_bios(self, context):
        logging.info(""

    def process_investor_bios(self, context):
        logging.info("Identifying investors...")
        self.cyphers.identify_investors_bios(self, context)

    def process_podcaster_bios(self, context):
        logging.info("Identifying podcasters based on bios...")
        self.cyphers.identify_podcasters_bios(self, context)
    
    def process_twitter_influencers(self, context):
        logging.info("Identifying cutoff for Twitter influencers...")
        cutoff = self.cyphers.identify_twitter_influencer_cutoff(self, context)
        logging.info("identifying influencers omg...")
        self.cyphers.identify_twitter_influencers_bios(self, context, cutoff=cutoff)

    


    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = ProfessionalsAnalysis()
    analysis.run()





