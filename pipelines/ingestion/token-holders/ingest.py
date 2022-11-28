from ..helpers import Ingestor
from .cyphers import TokenHoldersCyphers

class TokenHoldersIngestor(Ingestor):
    def __init__(self, bucket_name="token-holders"):
        self.cyphers = TokenHoldersCyphers()
        super().__init__(bucket_name)

    def prepare_token_data(self):
        data = {
            "ERC20": [],
            "ERC721": [],
            "ERC1155": []
        }
        for tokenAddress in self.data["tokens"]:
            token = self.data["tokens"][tokenAddress]
            tmp = {
                "contractAddress": tokenAddress, 
                "symbol": token["symbol"],
                "decimal": token["decimal"]
            }
            data[token["contractType"].upper()].append(tmp)
        return data

    def ingest_tokens(self):
        token_data = self.prepare_token_data()
        for tokenType in token_data:
            urls = self.s3.save_json_as_csv(
                token_data[tokenType], self.bucket_name, f"ingestor_tokens_{tokenType}_{self.asOf}")
            self.cyphers.create_or_merge_tokens(urls, tokenType)

    def prepare_holdings_data(self):
        data = []
        for wallet in self.data["balances"]:
            for balance in self.data["balances"][balance]:
                contractAddress = balance["contractAddress"]
                balance = balance["tokenBalance"]
                if self.data["tokens"][contractAddress]["contractType"] == "erc20":
                    numericBalance = int(balance, 16) / 10**self.data["tokens"][contractAddress]["decimal"]
                else:
                    numericBalance = int(balance, 16)
                data.append({
                    "address": wallet,
                    "contractAddress": contractAddress,
                    "balance": balance,
                    "numericBalance": numericBalance
                })
        return data

    def ingest_holdings(self):
        holding_data = self.prepare_holdings_data()
        urls = self.s3.save_json_as_csv(holding_data, self.bucket_name, f"ingestor_holdings_{self.asOf}")
        self.cyphers.create_or_merge_tokens(urls)

    def run(self):
        self.ingest_tokens()
        self.ingest_holdings()
        self.save_metadata()

if __name__ == '__main__':
    ingestor = TokenHoldersIngestor()
    ingestor.run()