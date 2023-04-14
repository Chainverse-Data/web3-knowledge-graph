from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import ProfessionalsCyphers
import logging
 
class ProfessionalsAnalysis(WICAnalysis):
    """Identitifes people who "work" in Web3"""
    def __init__(self):
        self.conditions = {
            "Positions": {
                "Founder": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_founder_bios
                    },
                "Investor": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_investor_bios
                    },
                "Marketer": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_marketers_bios
                    },
                "SalesPartnerships": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_sales_partnerships
                    },
                "CommunityManager": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_community_people_bios
                    },
                "DeveloperRelationsLead": {
                        "types": [TYPES["professions"]],
                        "definition": "TBD",
                        "call": self.process_devrel_bios
                    }
            }
        }
        
        self.subgraph_name = "Professions"
        self.cyphers = ProfessionalsCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-professionals")

    def process_founder_bios(self, context):
        logging.info("Identifying founders based on bios..")
        queryString = "'founder' OR 'co-founder'"
        self.cyphers.identify_founders_bios(context, queryString)
 
    def process_sales_partnerships(self, context):
        logging.info("Identifying company officers...")
        queryString = "'VP of Sales' or 'BizDev' OR 'business development' OR 'partnerships'"  
        self.cyphers.identify_sales_partnerships(context, queryString)
 
    def process_investor_bios(self, context):
        logging.info("Identifying investors...")
        self.cyphers.identify_investors_bios(context)
 
    def process_marketers_bios(self, context):
        logging.info("Identifying marketing professionals...")
        queryString = """'Marketing' OR 'Marketer' OR 'brand'"""
        self.cyphers.identify_marketers_bios(context, queryString)
 
    def process_community_people_bios(self, context):
        logging.info("Identifying community people...")
        queryString = """'community lead' OR 'community manager'"""
        self.cyphers.identify_community_lead_bios(context, queryString) 
 
    def process_devrel_bios(self, context):
        logging.info("idenitfying devrel people...")
        queryString = """'devrel' OR 'developer relations' OR 'ecosystem lead'"""
        self.cyphers.identify_devrel_bios(context, queryString)
 

    def run(self):
        self.process_conditions()
 
if __name__ == '__main__':
    analysis = ProfessionalsAnalysis()
    analysis.run()
