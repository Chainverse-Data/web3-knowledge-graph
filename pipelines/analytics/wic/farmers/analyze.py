from .. import WICAnalysis
from ..WICAnalysis import TYPES
from .cyphers import FarmerCyphers
import pandas as pd
import logging

class IncentiveFarmerAnalysis(WICAnalysis):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.subgraph_name = "OpportunisticUsers"
        self.conditions = {
            "GovernanceFarming": {
                "SuspiciousSnapshot": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD", 
                        "weight": 0,
                        "call": self.process_suspicious_snapshot_daos
                    }
            }, 
            "MarketplaceFarming": {
                "Mirror": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD", 
                        "weight": 0,
                        "call": self.process_suspicious_mirror
                    }
            },
            "WashTrading": {
                "NftWashTrading": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD", 
                        "weight": 0,
                        "call": self.process_nft_wash_trading
                    }
            },
            "Spammers": {
                "SpamTokenDeployer": {
                        "types": [TYPES["experiences"]],
                        "definition": "TBD", 
                        "weight": 0,
                        "call": self.process_spam_contract_deployment
                    }
            }
        }
    
        
        self.cyphers = FarmerCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-farming")
        self.washTraders = pd.read_csv('pipelines/analytics/wic/farmers/data/wash_trading_202302.csv')
        self.susDao = pd.read_csv('pipelines/analytics/wic/farmers/data/susdao.csv')

    def process_suspicious_snapshot_daos(self, context):
        susDao = self.susDao
        self.cyphers.connect_suspicious_snapshot_daos(context)
    
    def process_suspicious_mirror(self, context):
        self.cyphers.remove_mirror_label()
        cutoff = self.cyphers.get_mirror_benchmark()
        self.cyphers.label_mirror(cutoff)
        self.cyphers.connect_suspicious_mirror(context)
        self.cyphers.clean()

    def process_nft_wash_trading(self, context):
        wash_traders = self.washTraders
        addresses = list(set(wash_traders['seller']))
        self.cyphers.identify_nft_wash_traders(context, addresses)
    
    def process_spam_contract_deployment(self, context):
        self.cyphers.identify_spam_contract_deployers(context)

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = IncentiveFarmerAnalysis()
    analysis.run()
