import logging
from .. import WICAnalysis
from .cyphers import TradersCypher
import pandas as pd
from ..WICAnalysis import TYPES

class CreatorsCollectorsAnalysis(WICAnalysis):
    """This class reads from a local"""

    def __init__(self):
        self.subgraph_name = 'Traders'
        self.conditions = {
           "SophisticatedTraders": {
                "SudoSwapPowerUser": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_sudo_power_users
                },
                "BlurPowerUser": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_blur_power_users
                },
                "NftCollateralizedBorrowers": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD",
                    "call": self.process_nft_collat_borrowers
                }
           }
        }

        self.cyphers = TradersCypher(self.subgraph_name, self.conditions)
        super().__init__("wic-traders")

        ## TODOO This will need to be automated to avoid relying on this list.
        self.sudo_power_users = pd.read_csv('pipelines/analytics/wic/traders/data/sudo_power_traders.csv')
        self.blur_power_users = pd.read_csv("pipelines/analytics/wic/traders/data/blur_power_traders.csv")
        self.nft_backed_borrowers = pd.read_csv("pipelines/analytics/wic/traders/data/nft_borrowers.csv")
  
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

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = CreatorsCollectorsAnalysis()
    analysis.run()
    

    

    
