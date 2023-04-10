from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import InfluencersCyphers
import logging
 
class ProfessionalsAnalysis(WICAnalysis):
    """Identitifes people who "work" in Web3"""
    def __init__(self):
        self.conditions = {
            "SocialMedia" : {
                "TwitterInfluencer": {
                        "types": [TYPES["influence"]],
                        "definition": "TBD",
                        "call": self.process_podcaster_bios
                    },
                "Podcaster": {
                        "types": [TYPES["influence"]],
                        "definition": "TBD",
                        "call": self.process_twitter_influencers
                    },
            }
        }
        self.subgraph_name = "Influencers"
        self.cyphers = InfluencersCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-influencers")
 
    def process_podcaster_bios(self, context):
        logging.info("Identifying podcasters based on bios...")
        queryString = "'podcasts' OR 'podcast'"
        self.cyphers.identify_podcasters_bios(context, queryString)
 
    def process_twitter_influencers(self, context):
        logging.info("identifying influencers omg..")
        self.cyphers.identify_twitter_influencers(context)

    def run(self):
        self.process_conditions()
 
if __name__ == '__main__':
    analysis = ProfessionalsAnalysis()
    analysis.run()
