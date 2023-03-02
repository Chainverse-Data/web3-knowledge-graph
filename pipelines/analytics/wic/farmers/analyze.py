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
            "Extensions": {
                "Cosigners": self.process_suspicious_cosigners
            }
        }
        
        self.cyphers = FarmerCyphers(self.subgraph_name, self.conditions)
        super().__init__("wic-farming")

    def process_suspicious_snapshot_daos(self, context):
        self.cyphers.connect_suspicious_snapshot_daos(context)
    
    def process_suspicious_mirror(self, context):
        benchmark = self.cyphers.get_mirror_benchmark()
        self.cyphers.connect_suspicious_mirror(context, benchmark)

    def process_suspicious_cosigners(self, context):
        self.cyphers.connect_cosigner_expansion(context)

    def run(self):
        self.process_conditions()

if __name__ == '__main__':
    analysis = IncentiveFarmerAnalysis()
    analysis.run()