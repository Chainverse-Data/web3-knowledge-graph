import logging
from .. import WICAnalysis
from .cyphers import CreatorsCollectorsCypher
import pandas as pd
from helpers.s3 import save_df_as_csv


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
            }
        }
        self.cyphers = CreatorsCollectorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-creators-collectors")

        # TODO: This will need to be automated to avoid relying on this list.
        self.seeds_addresses = list(pd.read_csv("pipelines/analytics/wic/creators-collectors/data/seeds.csv")['address'])
        self.sudo_power_users = pd.read_csv('pipelines/analytics/wic/creators-collectors/data/sudo.csv')

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

    def process_sudo_power_users(self, context):
        logging.info("Getting benchmark for sudo power users")
        sudo_users = self.sudo_power_users
        sudo_benchmark = sudo_users['total_volume'].quantile(0.8)
        logging.info(f"Benchmark value for Sudoswap power users: {sudo_benchmark}")
        logging.info("Getting sudo power users wallet addresses...")
        sudo_power_wallets = sudo_users.loc[sudo_users['total_volume'] > sudo_benchmark]
        logging.info("saving power users")
        urls = self.save_df_as_csv(self, sudo_power_wallets, bucket_name=self.bucket_name, file_name="sudo_power_wallets",
        max_lines=10000, max_size=10000000)
        logging.info("creating power user nodes")
        self.cyphers.create_sudo_power_users(self.sudo_power_users, urls=urls)
        logging.info("connecting power users")
        self.cyphers.connect_sudo_power_users(self.sudo_power_users, urls=urls)


    def run(self):
      #  self.process_conditions()
        self.process_sudo_power_users()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
