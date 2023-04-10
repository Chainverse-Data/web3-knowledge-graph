from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import InterestsCyphers
import pandas as pd
import logging

class InterestsAnalysis(WICAnalysis):
    """This class reads labels wallets by interests"""
    def __init__(self):
        self.subgraph_name = "Interests"
        self.conditions = {
            "ArtCultureEntertainment": {
                    "Music": {
                        "types": [TYPES["interests"]],
                        "definition": "TBD",
                        "call": self.process_music
                    },
                    "Gaming": {
                        "types": [TYPES['interests']],
                        "definition": "TBD",
                        "call": self.process_gaming
                    },
                    "Outdoors": {
                        "types": [TYPES['interests']],
                        "definition": "TBD",
                        "call": self.process_outdoors
                    },
                    "FilmVideo": {
                        "types": [TYPES['interests']], 
                        "definition": "TBD",
                        "call": self.process_film_video
                    },
                    "Photography": {
                        "types": [TYPES['interests']],
                        "definition": "TBD",
                        "call": self.process_photography
                    },
                    "CultureCommentary": {
                        "types": [TYPES['interests']],
                        "definition": "TBD",
                        "call": self.process_culture
                    },
                    "WritingPublishing": {
                        "types": [TYPES['interests']],
                        "definition": "TBD",
                        "call": self.process_writing_publishing
                    },
                },
            "ScienceTech": {
                "DataScience": {
                    "types": [TYPES['interests']],
                    "definition": "TBD",
                    "call": self.process_data_science
                },
                "DeSci": {
                    "types": [TYPES['interests']],
                    "definition": "TBD",
                    "call": self.process_desci
                }
            },
            "SocialJustice": {
                "DiversityEquityInclusion": {
                    "types": [TYPES['interests']],
                    "definition": "TBD",
                    "call": self.process_dei
                },
                "RegenerativeSystems": {
                    "types": [TYPES['interests']],
                    "definition": "TBD",
                    "call": self.process_regen
                },
                "Education": {
                    "types": [TYPES['interests']],
                    "definition": "TBD",
                    "call": self.process_education
                }
            }
        }
        self.cyphers = InterestsCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-interests")

    
    def process_music(self, context):
        logging.info("Finding people interested in music...")
        self.cyphers.find_music_interested(context)

    def process_gaming(self, context):
        logging.info("Finding people interested in gaming...")
        self.cyphers.find_gaming_interested(context)
    
    def process_outdoors(self, context):
        logging.info("Finding outdoors interested...")
        self.cyphers.find_outdoors_interested(context)

    def process_film_video(self, context):
        logging.info("Finding film  video...")
        self.cyphers.find_film_video(context)

    def process_photography(self, context):
        logging.info("finding photography...")
        self.cyphers.find_photography(context)

    def process_culture(self, context):
        logging.info("finding culturecommentary...")
        self.cyphers.find_culture(context)

    def process_writing_publishing(self,context):
        logging.info("finding writing/publishing..")
        self.cyphers.find_writing_publishing(context)

    def process_data_science(self, context):
        logging.info('finding data scientists...')
        self.cyphers.find_data_scientists(context)

    def process_data_science(self, context):
        logging.info('finding data scientists...')
        self.cyphers.find_data_scientists(context)

    def process_desci(self, context):
        logging.info("finding desci folks...")
        self.cyphers.find_desci(context)

    def process_dei(self, context):
        logging.info('finding dei')
        self.cyphers.find_dei(context)

    def process_regen(self, context):
        logging.info("finding regen")
        self.cyphers.find_regen(context)

    def process_education(self, context):
        logging.info('find education')
        self.cyphers.find_ed(context)

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = InterestsAnalysis()
    analysis.run()
