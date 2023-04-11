from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import ProfessionalsCyphers
import logging
 
class ProfessionalsAnalysis(WICAnalysis):
    """Identitifes people who "work" in Web3"""
    def __init__(self):
        self.conditions = {
           "Web3NativeRole": {
                "OpsAdmin": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD",
                        "call": self.process_org_wallet_deployers
                    },
                "GovernanceAdmin": {
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
           },
            "CompanyRole": {
                "Founder": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_founder_bios
                    },
                "Investing": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_investor_bios
                    },
                "Marketing": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_marketers_bios
                    },
                "CompanyLeadership": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_company_officer_bios
                    },
                "CommunityManagement": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_community_people_bios
                    },
                "DevRel": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_devrel_bios
                    }
            }
        }
        
        self.subgraph_name = "Professions"
        self.cyphers = ProfessionalsCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-professionals")
 
 
    def process_org_wallet_deployers(self, context):
        logging.info("Identifying DAO treasury & ops wallet deployers...")
        self.cyphers.get_org_wallet_deployers(context)
 
    def process_org_multisig_signers(self, context):
        logging.info("Identifying signers of organization multisigs...")
        self.cyphers.get_org_multisig_signers(context)
 
    def process_org_snapshot_contributors(self, context):
        logging.info("Identifying snapshot contributors...")
        self.cyphers.get_snapshot_contributors(context)
 
    def process_founder_bios(self, context):
        logging.info("Identifying founders based on bios..")
        queryString = "'founder' OR 'co-founder'"
        self.cyphers.identify_founders_bios(context, queryString)
 
    def process_company_officer_bios(self, context):
        logging.info("Identifying company officers...")
        queryString = "'VP' or 'Vice President' OR 'CEO' or 'Head of'"
        self.cyphers.identify_community_lead_bios(context, queryString)
 
    def process_investor_bios(self, context):
        logging.info("Identifying investors...")
        self.cyphers.identify_investors_bios(context)
 
    def process_marketers_bios(self, context):
        logging.info("Identifying marketing professionals...")
        queryString = """'Marketing' OR 'Marketer'"""
        self.cyphers.identify_marketers_bios(context, queryString)
 
    def process_community_people_bios(self, context):
        logging.info("Identifying community people...")
        queryString = """'community lead' OR 'community manager'"""
        self.cyphers.identify_community_lead_bios(context, queryString) 
 
    def process_devrel_bios(self, context):
        logging.info("idenitfying devrel people...")
        queryString = """'devrel' OR 'developer relations' OR 'ecosystem lead'"""
        self.cyphers.identify_devrel_bios(context, queryString)

    def process_dao_funding_recipient(self, context):
        logging.info("DAO funding recipients....")
        self.cyphers.get_dao_funding_recipients(context)
    
    def process_dao_treasury_funder(self, context):
        logging.info("DAO treasury funders...")
        self.cyphers.get_dao_treasury_funders(context)

    def process_technical_contributor(self, context):
        logging.info('get technical contributors...')
        self.cyphers.get_technical_contributors(context)
 

    def run(self):
        self.process_conditions()
 
if __name__ == '__main__':
    analysis = ProfessionalsAnalysis()
    analysis.run()
