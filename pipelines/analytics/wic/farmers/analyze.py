from .. import WICAnalysis
from .cyphers import FarmerCyphers

class IncentiveFarmerAnalysis(WICAnalysis):
    """This class reads from the Neo4J instance for Twitter nodes to call the Twitter API and retreive extra infos"""
    def __init__(self):
        self.subgraph_name = "IncentiveFarming"
        self.conditions = {
            "GovernanceFarming": {
                "SuspiciousSnapshot": self.process_suspicious_snapshot_daos
            }, 
            "MarketplaceFarming": {
                "Mirror": self.process_suspicious_mirror
            },
            "WashTrading": {
                "NftWashTrading": self.process_nft_wash_trading
            },
            "Spammers": {
                "SpamContractDeployer": self.process_spam_contract_deployment
            },
            "FarmersAffiliates": {
                "FarmerCosigner": self.process_suspicious_cosigners
        }
    }
        
        self.cyphers = FarmerCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-farming")

    def process_suspicious_snapshot_daos(self, context):
        self.cyphers.connect_suspicious_snapshot_daos(context)
    
    def process_suspicious_mirror(self, context):
        self.cyphers.remove_mirror_label()
        cutoff = self.cyphers.get_mirror_benchmark()
        self.cyphers.label_mirror(cutoff)
        self.cyphers.connect_suspicious_mirror(context)
        self.cyphers.clean()

    def process_nft_wash_trading(self, context):
        self.cyphers.identify_nft_wash_traders(context)
    
    def process_spam_contract_deployment(self, context):
        self.cyphers.identify_spam_contract_deployers(context)

    def process_suspicious_cosigners(self, context):
        self.cyphers.connect_cosigner_expansion(context)

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = IncentiveFarmerAnalysis()
    analysis.run()