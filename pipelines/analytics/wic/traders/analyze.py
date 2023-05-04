import logging
from .. import WICAnalysis
from .cyphers import TradersCyphers
import pandas as pd
from ..WICAnalysis import TYPES

class TradersAnalysis(WICAnalysis):
    """This class reads from a local"""

    def __init__(self):
        self.subgraph_name = 'Traders'
        self.conditions = {
           "PowerTraderMarketplaces": {
                "SudoSwapPowerUser": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD", 
                    "weight": .75,
                    "call": self.process_sudo_power_users
                },
                "BlurPowerUser": {
                    "types": [TYPES['experiences']],
                    "definition": "TBD", 
                    "weight": .75,
                    "call": self.process_blur_power_users
                }
           },
              "NftCollateralizedBorrower": {
                "x2y2Borrower": {
                "types": [TYPES['experiences']],
                "definition": "Borrower on x2y2",
                "weight": .7,
                "call": self.process_x2y2_borrowers
                },
                "ParaspaceBorrower": {
                    "types": [TYPES['experiences']],
                    "definition": "Borrower on Paraspace",
                    "weight": .7,
                    "call": self.process_paraspace_borrowers
                },
                "ArcadeBorrower": {
                    "types": [TYPES['experiences']],
                    "definition": "Borrower on Arcade.xyz",
                    "weight": .7,
                    "call": self.process_arcade_borrowers
                },
                "NftfiBorrower": {
                    "types": [TYPES['experiences']],
                    "definition": "Borrower on NFTfi",
                    "weight": .7,
                    "call": self.process_nftfi_borrowers
                },
                "BendBorrower": {
                    "types": [TYPES['experiences']],
                    "definition": "Borrower on Bend",
                    "weight": .7,
                    "call": self.process_bend_borrowers
                }
              },
              "NftCollateralizedLender": {
                "x2y2Lender": {
                "types": [TYPES['experiences']],
                "definition": "Lender on x2y2",
                "weight": .65,
                "call": self.process_x2y2_lenders
                },
                "ParaspaceLender": {
                    "types": [TYPES['experiences']],
                    "definition": "Lender on Paraspace",
                    "weight": .65,
                    "call": self.process_paraspace_lenders
                },
                "ArcadeLender": {
                    "types": [TYPES['experiences']],
                    "definition": "Lender on Arcade.xyz",
                    "weight": .65,
                    "call": self.process_arcade_lenders
                },
                "NftfiLender": {
                    "types": [TYPES['experiences']],
                    "definition": "Lender on NFTfi",
                    "weight": .65,
                    "call": self.process_nftfi_lenders
                },
                "BendLender": {
                    "types": [TYPES['experiences']],
                    "definition": "Lender on Bend",
                    "weight": .65,
                    "call": self.process_bend_lenders
                }
              },

        }

        self.cyphers = TradersCyphers(self.subgraph_name, self.conditions)
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

    def process_x2y2_borrowers(self, context):
        logging.info("Getting X272 borrowers...")
        self.cyphers.connect_x2y2_borrowers(context)

    def process_paraspace_borrowers(self, context):
        logging.info("Getting paraspace borrowers...")
        self.cyphers.connect_paraspace_borrowers(context)

    def process_arcade_borrowers(self, context):
        logging.info("Getting Arcade borrowers...")
        self.cyphers.connect_arcade_borrowers(context)

    def process_bend_borrowers(self, context):
        logging.info("Getting Bend borrowers...")
        self.cyphers.connect_bend_borrowers(context)

    def process_nftfi_borrowers(self, context):
        logging.info("Getting NFTfi borrowers...")
        self.cyphers.connect_nftfi_borrowers(context)

    def process_x2y2_lenders(self, context):
        logging.info("Getting X272 lenders...")
        self.cyphers.connect_x2y2_lenders(context)

    def process_paraspace_lenders(self, context):
        logging.info("Getting paraspace lenders...")
        self.cyphers.connect_paraspace_lenders(context)

    def process_arcade_lenders(self, context):
        logging.info("Getting Arcade lenders...")
        self.cyphers.connect_arcade_lenders(context)

    def process_bend_lenders(self, context):
        logging.info("Getting Bend lenders...")
        self.cyphers.connect_bend_lenders(context)

    def process_nftfi_lenders(self, context):
        logging.info("Getting NFTfi lenders...")
        self.cyphers.connect_nftfi_lenders(context)


    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    analysis = TradersAnalysis()
    analysis.run()
    

    

    
