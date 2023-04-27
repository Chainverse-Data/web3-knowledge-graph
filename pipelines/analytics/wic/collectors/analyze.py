import logging
from .. import WICAnalysis
from .cyphers import CreatorsCollectorsCypher
import pandas as pd
from ..WICAnalysis import TYPES

class CreatorsCollectorsAnalysis(WICAnalysis):
    """This class reads from a local"""

    def __init__(self):
        self.subgraph_name = 'Collectors'
        self.conditions = {
           "Collectors": {
               "Web3WritingCollector": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_mirror_collectors
               },
               "BlueChipNftCollector": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_blue_chip_nfts
               },
               "ThreeLetterEnsName": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_three_ens
               },
               "Web3MusicCollector": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_web3_music_collectors
               }
           }
        }

        self.cyphers = CreatorsCollectorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-collectors")

        ## TODO This will need to be automated to avoid relying on this list.
        self.seeds_addresses = list(pd.read_csv("pipelines/analytics/wic/collectors/data/bluechip_20230427.csv")['address'])

    def process_blue_chip_nfts(self, context):
        logging.info("Identifying blue chips...")
        self.cyphers.cc_blue_chip(self.seeds_addresses, context)

    def process_three_ens(self, context):
        logging.info("Identifying wallets that hold three letter ENS Names")
        self.cyphers.three_letter_ens(context)

    def process_mirror_collectors(self, context):
        logging.info("Idenitfying legit Mirror NFT collectors...")
        self.cyphers.get_mirror_collectors(context)

    def process_web3_music_collectors(self, context):
        logging.info("getting web3 music collectors...")
        self.cyphers.get_web3_music_collectors(context)

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
