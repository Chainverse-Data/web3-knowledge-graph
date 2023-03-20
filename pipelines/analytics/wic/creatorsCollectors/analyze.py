import logging
from .. import WICAnalysis
from .cyphers import CreatorsCollectorsCypher
import pandas as pd

class CreatorsCollectorsAnalysis(WICAnalysis):
    """This class reads from a local"""

    def __init__(self):
        self.subgraph_name = 'CreatorsCollectors'
        self.conditions = {
            "Writing": {
                "MirrorAuthor": self.process_writing
            },
            "BlueChip": {
                "BlueChipNFTCollections": self.process_NFTs_blue_chip,
                "ThreeLetterEnsName": self.process_three_ens
            },
        }
        self.cyphers = CreatorsCollectorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-creators-collectors")

        # TODO: This will need to be automated to avoid relying on this list.
        self.seeds_addresses = list(pd.read_csv("pipelines/analytics/wic/creators-collectors/data/seeds.csv")['address'])

    def process_writing(self, context):
        benchmark = self.cyphers.get_writers_benchmark()
        logging.info(f"Benchmark value for Mirror articles: {benchmark}")
        self.cyphers.cc_writers(context, benchmark)

    def process_NFTs_blue_chip(self, context):
        benchmark = self.cyphers.get_bluechip_benchmark(self.seeds_addresses)
        logging.info(f"Benchmark value for Blue Chip NFTs: {benchmark}")
        self.cyphers.cc_blue_chip(self.seeds_addresses, context, benchmark)

    def process_three_ens(self, context):
        logging.info("Identifying wallets that hold three letter ENS Names")
        self.cyphers.three_letter_ens(context)

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
