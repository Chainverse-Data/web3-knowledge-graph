from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import InfluencersCyphers
import logging
 
class InfluencersAnalysis(WICAnalysis):
    """Identitifes people who "work" in Web3"""
    def __init__(self):
        self.conditions = {
            "ContentCreation" : {
                "MirrorInfluencer": {
                    "types": [TYPES["influence"]],
                    "definition": "TBD",
                    "call": self.process_mirror_influencer
                },
                "SubstackInfluencer": {
                    "types": [TYPES["influence"]],
                    "definition": "TBD",
                    "call": self.process_substack_influencer
                },
                "DuneWizard": {
                    "types": [TYPES["influence"]],
                    "definition": "TBD",
                    "call": self.process_dune_wizard
                },
                "InfluentialPodcaster": {
                    "types": [TYPES['influence']],
                    "definition": "TBD",
                    "call": self.process_podcasters
                }
            },
            "SocialMedia": {
                "TwitterInfluencer": {
                        "types": [TYPES["influence"]],
                        "definition": "TBD",
                        "call": self.process_twitter_influencers
                    }
            }
        }
        self.subgraph_name = "Influencers"
        self.cyphers = InfluencersCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-influencers")


    def process_mirror_influencer(self, context):
        logging.info("getting mirror influencers...")
        self.cyphers.get_mirror_influencer(context)
    
    def process_substack_influencer(self, context):
        logging.info("getting substack influencers...")
        self.cyphers.get_substack_influencer(context)
 
    def process_podcasters(self, context):
        logging.info("Identifying podcasters based on bios...")
        self.cyphers.identify_podcasters(context)

    def process_dune_wizard(self, context):
        logging.info("Process dune wizards...")
        self.cyphers.get_dune_influencers(context)

    def process_twitter_influencers(self, context):
        logging.info("identifying influencers omg..")
        self.cyphers.identify_twitter_influencers(context)

    def run(self):
        self.process_conditions()
 
if __name__ == '__main__':
    analysis = InfluencersAnalysis()
    analysis.run()
