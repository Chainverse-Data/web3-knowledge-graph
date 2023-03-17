from ..helpers import Ingestor
from .cyphers import NFTfiCyphers

class NFTfiIngestor(Ingestor):
    def __init__(self):
        self.cyphers = NFTfiCyphers()
        super().__init__("nftfi")

    def prepare_extra_data(self):
        wallets = set()
        ERC721s = set()
        ERC20s = set()
        for loan in self.scraper_data["LoanStarted"]:
            wallets.add(loan["borrower"])
            wallets.add(loan["lender"])
            ERC721s.add(loan["nftCollateralContract"])
            ERC20s.add(loan["loanERC20Denomination"])
        wallets = [{"address": key} for key in wallets]
        ERC721s = [{"contractAddress": key} for key in ERC721s]
        ERC20s = [{"contractAddress": key} for key in ERC20s]
        return wallets, ERC20s, ERC721s

    def ingest_loans(self):
        wallets, ERC20s, ERC721s = self.prepare_extra_data()
        urls = self.save_json_as_csv(wallets, self.bucket_name, f"ingestor_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        urls = self.save_json_as_csv(ERC20s, self.bucket_name, f"ingestor_ERC20s_{self.asOf}")
        self.cyphers.queries.create_or_merge_tokens(urls, "ERC20")
        urls = self.save_json_as_csv(ERC721s, self.bucket_name, f"ingestor_ERC721s_{self.asOf}")
        self.cyphers.queries.create_or_merge_tokens(urls, "ERC721")

        urls = self.save_json_as_csv(self.scraper_data["LoanStarted"], self.bucket_name, f"ingestor_loan_started_{self.asOf}")
        self.cyphers.create_or_merge_loans(urls)
        self.cyphers.link_or_merge_loans_borrowers(urls)
        self.cyphers.link_or_merge_loans_lenders(urls)
        self.cyphers.link_or_merge_loans_collateral(urls)
        self.cyphers.link_or_merge_loans_demonination(urls)

        urls = self.save_json_as_csv(self.scraper_data["LoanRepaid"], self.bucket_name, f"ingestor_loan_repaid_{self.asOf}")
        self.cyphers.set_loan_status(urls, "Repaid")

        urls = self.save_json_as_csv(self.scraper_data["LoanLiquidated"], self.bucket_name, f"ingestor_loan_liquidated_{self.asOf}")
        self.cyphers.set_loan_status(urls, "Liquidated")

    def run(self):
        self.ingest_loans()

if __name__ == "__main__":
    ingestor = NFTfiIngestor()
    ingestor.run()
