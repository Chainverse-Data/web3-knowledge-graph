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
               "WritingNftCreator": self.process_writing,
               "WritingNftCollector": self.process_writing_collectors
           },
            "BlueChip": {
               "BlueChipNFTCollections": self.process_NFTs_blue_chip
           },"Rarity": {
               "ThreeLetterEns": self.process_three_ens
            },
            "NftMarketplacePowerUsers": {
                "SudoswapPowerUser": self.process_sudo_power_users,
                "BlurPowerUser": self.process_blur_power_users,
                "NftLending": self.process_nft_lending_users
            }
        }
        ## add artists, musicians, project affiliates (?)
        self.cyphers = CreatorsCollectorsCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-creators-collectors")

        ## TODOO This will need to be automated to avoid relying on this list.
        self.seeds_addresses = list(pd.read_csv("pipelines/analytics/wic/creatorsCollectors/data/seeds.csv")['address'])
        self.sudo_power_users = pd.read_csv('pipelines/analytics/wic/creatorsCollectors/data/sudo_power_trader.csv')
        self.blur_power_users = pd.read_csv("pipelines/analytics/wic/creatorsCollectors/data/blur_power_trading.csv")
        self.nft_lending = pd.read_csv("pipelines/analytics/wic/creatorsCollectors/data/nft_lending_feb.csv")


    def process_writing(self, context):
        logging.info(f"Identifying writers...")
        self.cyphers.cc_writers(context)

    def process_writing_collectors(self, context):
        logging.info("""logging Mirror article collectors...""")
        self.cyphers.get_writing_nft_collectors(context)

    def process_NFTs_blue_chip(self, context):
        logging.info("Identifying blue chips...")
        self.cyphers.cc_blue_chip(self.seeds_addresses, context)

    def process_three_ens(self, context):
        logging.info("Identifying wallets that hold three letter ENS Names")
        self.cyphers.three_letter_ens(context)

    def process_sudo_power_users(self, context):
        logging.info("Getting benchmark for sudo power users")
        sudo_users = self.sudo_power_users
        sudo_users = sudo_users.dropna(subset=['address'])
        urls = self.save_df_as_csv(sudo_users, bucket_name=self.bucket_name, file_name=f"sudo_power_wallets_{self.asOf}")
        logging.info("creating power user nodes")
        self.cyphers.create_sudo_power_users(urls)
        logging.info("connecting power users")
        self.cyphers.connect_sudo_power_users(context, urls)

    def process_blur_power_users(self, context):
        logging.info("Saving NFT marketplace power users....")
        blur_users = self.blur_power_users
        blur_users = blur_users.dropna(subset=['address'])
        logging.info(blur_users.head(5))
        urls = self.save_df_as_csv(blur_users, bucket_name=self.bucket_name, file_name=f"blur_power_wallets_{self.asOf}")
        logging.info(urls)
        logging.info("creating blur nodes...")
        self.cyphers.create_blur_power_users(urls)
        logging.info("connecting blur power users")
        self.cyphers.connect_blur_power_users(context, urls)

    def process_nft_lending_users(self, context):
        logging.info("Saving NFT lenders and borrowers...")
        nft_lending = self.nft_lending
        nft_lending = nft_lending.dropna(subset=['address'])
        urls = self.save_df_as_csv(nft_lending, bucket_name=self.bucket_name, file_name=f"nft_backed_borrowers{self.asOf}")
        logging.info("creating NFT backed borrowers...")
        self.cyphers.create_nft_borrowers(context, urls)
        logging.info('connect NFT backed borrowers')
        self.cyphers.connect_nft_borrowers(context, urls)
        logging.info("I am done cuz")

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    
