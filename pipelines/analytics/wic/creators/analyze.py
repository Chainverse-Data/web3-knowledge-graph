import logging
from .. import WICAnalysis
from .cyphers import CreatorsCypher
import pandas as pd
from ..WICAnalysis import TYPES

class CreatorsCollectorsAnalysis(WICAnalysis):
    """This class reads from a local"""

    def __init__(self):
        self.subgraph_name = 'Creators'
        self.conditions = {
           "Creators": {
                "Web3Writer": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_writing
                },
                "Web3Musician": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_web3_musicians
                },
                "Web3DataAnalyst": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_dune_wizards
                }
           }
        }

        self.cyphers = CreatorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-creators")

    def process_writing(self, context):
        benchmark = self.cyphers.get_writers_benchmark()
        logging.info(f"Benchmark value for Mirror articles: {benchmark}")
        self.cyphers.cc_writers(context, benchmark)

    def process_web3_musicians(self, context):
        logging.info("Finding web3 musicians...")
        self.cyphers.get_web3_musicians(context)

    def process_dune_wizards(self, context):
        logging.info("getting dune people")
        self.cyphers.web3_data_analysts(context)

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
