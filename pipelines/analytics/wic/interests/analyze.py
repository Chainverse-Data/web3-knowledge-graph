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
            "Art_Culture_Entertainment": {
                    "Music": {
                        "type": TYPES["interests"],
                        "definition": "TBD",
                        "call": self.process_music
                    },
                    "Gaming": {
                        "type": TYPES['interests'],
                        "definition": "TBD",
                        "call": self.process_gaming
                    },
                    "Outdoors": {
                        "type": TYPES['interests'],
                        "definition": "TBD",
                        "call": self.process_outdoors
                    },
                    "Film_Video": {
                        "type": TYPES['interests'], 
                        "definition": "TBD",
                        "call": self.process_film_video
                    },
                    "Photography": {
                        "type": TYPES['interests'],
                        "definition": "TBD",
                        "call": self.process_photography
                    },
                    "Culture_Commentary": {
                        "type": TYPES['interests'],
                        "definition": "TBD",
                        "call": self.process_culture
                    },
                    "Writing_Publishing": {
                        "type": TYPES['interests'],
                        "definition": "TBD",
                        "call": self.process_writing_publishing
                    },
                },
            "Science_Tech": {
                "Data_Science": {
                    "type": TYPES['interests'],
                    "definition": "TBD",
                    "call": self.process_data_science
                },
                "DeSci": {
                    "type": TYPES['interests'],
                    "definition": "TBD",
                    "call": self.process_desci
                }
            },
            "Social_Justice": {
                "Diversity_Equity_Inclusion": {
                    "type": TYPES['interests'],
                    "definition": "TBD",
                    "call": self.process_dei
                },
                "Regenerative_Systems": {
                    "type": TYPES['interests'],
                    "definition": "TBD",
                    "call": self.process_regen
                },
                "Education": {
                    "type": TYPES['interests'],
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
