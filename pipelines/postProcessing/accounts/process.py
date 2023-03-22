

from .cyphers import AccountsCyphers
from ..helpers import Processor

class AccountsProcessor(Processor):
    def __init__(self):
        self.cyphers = AccountsCyphers()
        self.account_types_labels = ["Wallet", "Twitter", "Github", "Gitcoin", "Email", "Farcaster"]
        super().__init__(bucket_name="accounts-processing")

    def process_acount_types(self):
        for label in self.account_types_labels:
            self.cyphers.set_account_type(label)

    def process_wallets_account_labeling(self):
        self.cyphers.set_wallet_account_label()

    def process_twitter_accounts(self):
        self.cyphers.link_wallet_twitter_accounts()

    def process_github_accounts(self):
        self.cyphers.link_wallet_github_accounts()

    def process_same_handle(self):
        self.cyphers.link_same_handles()

    def process_mirror_author_ref_twitter(self):
        self.cyphers.link_mirror_authors_to_twitter(threshold=3, proportion=0.8)

    def run(self):
        self.process_wallets_account_labeling()
        self.process_acount_types()
        self.process_twitter_accounts()
        self.process_github_accounts()
        self.process_same_handle()
        self.process_mirror_author_ref_twitter()

if __name__ == "__main__":
    P = AccountsProcessor()
    P.run()