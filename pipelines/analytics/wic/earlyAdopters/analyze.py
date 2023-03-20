
from .cyphers import EarlyAdoptersCyphers
from .. import WICAnalysis

class EarlyAdopterAnalysis(WICAnalysis):
    def __init__(self) -> None:

        self.subgraph_name = "EarlyAdpoters"

        self.conditions = {
            "EarlyDappUser": {
                "EarlyDaoHaus": self.process_early_daohaus_users,
                "EarlySnapshot": self.process_early_snapshot_users,
            },
            "DappPowerUser": {
                "DaoSummoner": self.process_dao_summoners,
                "DaoAdmin": self.process_dao_admins,
                "NftSubscriptionAdmin": self.process_nft_sub_admins
            }
        }

        self.cyphers = EarlyAdoptersCyphers(self.subgraph_name, self.conditions) 

        super().__init__("wic-early-adopters")

    def process_early_daohaus_users(self, context):
        self.cyphers.connect_early_daohaus_users(context)

    def process_early_snapshot_users(self, context):
        self.cyphers.connect_early_snapshot_users(context)

    def process_dao_summoners(self, context):
        self.cyphers.connect_dao_summoners(context)

    def process_dao_admins(self, context):
        self.cyphers.connect_dao_admins(context)

    def process_nft_sub_admins(self, context):
        self.cyphers.connect_nft_subscriptions_admins(context)

    def run(self):
        self.process_conditions()

if __name__ == "__main__":
    A = EarlyAdopterAnalysis()
    A.run()
