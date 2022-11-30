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
        for tokenAddress in self.scraper_data["tokens"]:
            token = self.scraper_data["tokens"][tokenAddress]
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
        for wallet in self.scraper_data["balances"]:
            for balance in self.scraper_data["balances"][wallet]:
                if "error" not in balance:
                    contractAddress = balance["contractAddress"]
                    decimal = self.scraper_data["tokens"][contractAddress]["decimal"]
                    if type(decimal) == str and "0x" in decimal:
                        decimal = int(decimal, 16)
                    elif type(decimal) == str:
                        decimal = int(decimal)
                    if self.scraper_data["tokens"][contractAddress]["contractType"] == "erc20" and decimal:
                        numericBalance = int(balance["tokenBalance"], 16) / 10**decimal
                    else:
                        numericBalance = int(balance["tokenBalance"], 16)
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
        self.cyphers.link_wallet_tokens(urls)

    def run(self):
        self.ingest_tokens()
        self.ingest_holdings()
        self.save_metadata()

if __name__ == '__main__':
    ingestor = TokenHoldersIngestor()
    ingestor.run()