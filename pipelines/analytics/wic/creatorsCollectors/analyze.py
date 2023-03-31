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
           "Writing": {
               "MirrorAuthor": {
                   "type": TYPES["influence"],
                   "call": self.process_writing,
               } 
               
           },
            "BlueChip": {
               "BlueChipNFTCollections": {
                   "type": TYPES["experiences"],
                   "call": self.process_NFTs_blue_chip,
               } 
           },"Rarity": {
               "ThreeLetterEns": {
                   "type": TYPES["experiences"],
                   "call": self.process_three_ens,
               } 
            },
            "NftMarketplacePowerUsers": {
                "SudoswapPowerUser": {
                   "type": TYPES["experiences"],
                   "call": self.process_sudo_power_users,
                },
                "BlurPowerUser": {
                   "type": TYPES["experiences"],
                   "call": self.process_blur_power_users,
                },
                "NftCollateralizedBorrower": {
                   "type": TYPES["interests"],
                   "call": self.process_nft_collat_borrowers,
                }
            }
        }
        self.cyphers = CreatorsCollectorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-creators-collectors")

        ## TODOO This will need to be automated to avoid relying on this list.
        self.seeds_addresses = list(pd.read_csv("pipelines/analytics/wic/creators-collectors/data/seeds.csv")['address'])
        self.sudo_power_users = pd.read_csv('pipelines/analytics/wic/creators-collectors/data/sudo.csv')
        self.blur_power_users = pd.read_csv("pipelines/analytics/wic/creators-collectors/data/blur.csv")
        self.nft_backed_borrowers = pd.read_csv("pipelines/analytics/wic/creators-collectors/data/nft_borrowers.csv")

    def process_writing(self, context):
        benchmark = self.cyphers.get_writers_benchmark()
        logging.info(f"Benchmark value for Mirror articles: {benchmark}")
        self.cyphers.cc_writers(context, benchmark)

    def process_NFTs_blue_chip(self, context):
        logging.info("Identifying blue chips...")
        self.cyphers.cc_blue_chip(self.seeds_addresses, context)

    def process_three_ens(self, context):
        logging.info("Identifying wallets that hold three letter ENS Names")
        self.cyphers.three_letter_ens(context)

    def process_sudo_power_users(self, context):
        logging.info("Getting benchmark for sudo power users")
        sudo_users = self.sudo_power_users.dropna(subset=['seller'])
        sudo_benchmark = sudo_users['total_volume'].quantile(0.8)
        logging.info(f"Benchmark value for Sudoswap power users: {sudo_benchmark}")
        logging.info("Getting sudo power users wallet addresses...")
        sudo_power_wallets = sudo_users.loc[sudo_users['total_volume'] > sudo_benchmark]
        sudo_power_wallets["address"] = sudo_power_wallets["seller"].unique()
        urls = self.save_df_as_csv(sudo_power_wallets, bucket_name=self.bucket_name, file_name=f"sudo_power_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.connect_sudo_power_users(context, urls)

    def process_blur_power_users(self, context):
        logging.info("Saving NFT marketplace power users....")
        blur_users = self.blur_power_users.dropna(subset=['address'])
        blur_users = blur_users.drop_duplicates(['address'])
        urls = self.save_df_as_csv(blur_users, bucket_name=self.bucket_name, file_name=f"blur_power_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.connect_blur_power_users(context, urls)

    def process_nft_collat_borrowers(self, context):
        logging.info("Saving NFT-backed borrowers...")
        nft_backed_borrowers = self.nft_backed_borrowers.dropna(subset=['address'])
        nft_backed_borrowers = nft_backed_borrowers.drop_duplicates(["address"])
        urls = self.save_df_as_csv(nft_backed_borrowers, bucket_name=self.bucket_name, file_name=f"nft_backed_borrowers{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.connect_nft_borrowers(context, urls)

    def process_mirror_collectors(self, context):
        logging.info("Idenitfying legit Mirror NFT collectors...")
        self.cyphers.get_mirror_collectors(context)

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
