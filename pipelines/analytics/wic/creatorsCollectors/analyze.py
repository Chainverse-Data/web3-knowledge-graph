import logging
from .. import WICAnalysis
from .cyphers import CreatorsCollectorsCypher
import pandas as pd
from ..WICAnalysis import TYPES

class CreatorsCollectorsAnalysis(WICAnalysis):
    """This class reads from a local"""

    def __init__(self):
        self.subgraph_name = 'CreatorsCollectors'
        self.conditions = {
           "Collectors": {
               "Web3WritingCollector": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_mirror_collectors
               },
               "BlueChipNftCollector": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_blue_chip_nfts
               },
               "ThreeLetterEnsName": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_three_ens
               },
               "Web3MusicCollector": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_web3_music_collectors
               }
           },
           "Creators": {
                "Web3Writer": {
                    "type": TYPES['influence'],
                    "definition": "TBD",
                    "call": self.process_writing
                },
                "Web3Musician": {
                    "type": TYPES['influence'],
                    "definition": "TBD",
                    "call": self.process_web3_musicians
                },
                "DuneDashboardWizard": {
                    "type": TYPES['influence'],
                    "definition": "TBD",
                    "call": self.process_dune_wizards
                }
           },
           "SophisticatedTraders": {
                "SudoSwapPowerUser": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_sudo_power_users
                },
                "BlurPowerUser": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_blur_power_users
                },
                "NftCollateralizedBorrowers": {
                    "type": TYPES['experiences'],
                    "definition": "TBD",
                    "call": self.process_nft_collat_borrowers
                }
           }
        }

        self.cyphers = CreatorsCollectorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-creators-collectors")

        ## TODOO This will need to be automated to avoid relying on this list.
        self.seeds_addresses = list(pd.read_csv("pipelines/analytics/wic/creatorsCollectors/data/seeds.csv")['address'])
        self.sudo_power_users = pd.read_csv('pipelines/analytics/wic/creatorsCollectors/data/sudo_power_traders.csv')
        self.blur_power_users = pd.read_csv("pipelines/analytics/wic/creatorsCollectors/data/blur_power_traders.csv")
        self.nft_backed_borrowers = pd.read_csv("pipelines/analytics/wic/creatorsCollectors/data/nft_borrowers.csv")

    def process_writing(self, context):
        benchmark = self.cyphers.get_writers_benchmark()
        logging.info(f"Benchmark value for Mirror articles: {benchmark}")
        self.cyphers.cc_writers(context, benchmark)

    def process_blue_chip_nfts(self, context):
        logging.info("Identifying blue chips...")
        self.cyphers.cc_blue_chip(self.seeds_addresses, context)

    def process_three_ens(self, context):
        logging.info("Identifying wallets that hold three letter ENS Names")
        self.cyphers.three_letter_ens(context)

    def process_sudo_power_users(self, context):
        logging.info("Getting benchmark for sudo power users")
        sudo_power_users = self.sudo_power_users
        logging.info("Getting sudo power users wallet addresses...")
        urls = self.save_df_as_csv(sudo_power_users, file_name=f"sudo_power_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.connect_sudo_power_users(context, urls)

    def process_blur_power_users(self, context):
        logging.info("Saving NFT marketplace power users....")
        blur_power_users = self.blur_power_users.dropna(subset=['address'])
        urls = self.save_df_as_csv(blur_power_users, file_name=f"blur_power_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.connect_blur_power_users(context, urls)

    def process_nft_collat_borrowers(self, context):
        logging.info("Saving NFT-backed borrowers...")
        nft_backed_borrowers = self.nft_backed_borrowers.dropna(subset=['address'])
        urls = self.save_df_as_csv(nft_backed_borrowers, file_name=f"nft_backed_borrowers{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.connect_nft_borrowers(context, urls)

    def process_mirror_collectors(self, context):
        logging.info("Idenitfying legit Mirror NFT collectors...")
        self.cyphers.get_mirror_collectors(context)

    def process_web3_musicians(self, context):
        logging.info("Finding web3 musicians...")
        self.cyphers.get_web3_musicians(context)

    def process_web3_music_collectors(self, context):
        logging.info("getting web3 music collectors...")
        self.cyphers.get_web3_music_collectors(context)

    def process_dune_wizards(self, context):
        logging.info("getting dune people")
        self.cyphers.get_dune_dashboard_wizards(context)

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
