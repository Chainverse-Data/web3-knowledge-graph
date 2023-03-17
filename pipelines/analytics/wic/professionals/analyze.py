
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
                "OrgEnsCustodian": self.process_org_ens_admin
            },
            "Web3Professionals": {
                "Founders": self.process_founder_bios,
                "Investors": self.process_investor_bios,
                "Marketers": self.process_marketers_bios,
                "CompanyOfficer": self.process_company_officer_bios,
                "CommunityLeads": self.process_community_people_bios,
                "DeveloperRelations": self.process_devrel_bios
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
        self.cyphers.get_org_wallet_deployers(context)
    
    def process_org_multisig_signers(self, context):
        logging.info("Identifying signers of organization multisigs...")
        self.cyphers.get_org_multisig_signers(context)
    
    def process_org_snapshot_contributors(self, context):
        logging.info("Identifying snapshot contributors...")
        self.cyphers.get_snapshot_contributors(context)

    def process_org_ens_admin(self, context):
        logging.info("Identifying ENS administrators...")
        self.cyphers.get_ens_admin(context)

    def process_founder_bios(self, context):
        logging.info("Identifying founders based on bios..")
        self.cyphers.identify_founders_bios(context)

    def process_company_officer_bios(self, context):
        logging.info("Identifying company officers...")
        self.cyphers.identify_community_lead_bios(context)

    def process_investor_bios(self, context):
        logging.info("Identifying investors...")
        self.cyphers.identify_investors_bios(context)

    def process_marketers_bios(self, context):
        logging.info("Identifying marketing professionals...")
        self.cyphers.identify_marketers_bios(context)

    def process_community_people_bios(self, context):
        logging.info("Identifying community people...")
        self.cyphers.identify_community_lead_bios(context)

    def process_devrel_bios(self, context):
        logging.info("idenitfying devrel people...")
        self.cyphers.identify_devrel_bios(context)

    def process_podcaster_bios(self, context):
        logging.info("Identifying podcasters based on bios...")
        self.cyphers.identify_podcasters_bios(context)
    
    def process_twitter_influencers(self, context):
        logging.info("Identifying cutoff for Twitter influencers...")
        self.cyphers.identify_twitter_influencers_bios(context)
        logging.info("identifying influencers omg...")

    def connect_shit(self):
        logging.info("Connecting wallets...")
        self.cyphers.connect_accounts_to_wallet_for_bios()

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = ProfessionalsAnalysis()
    analysis.run()





