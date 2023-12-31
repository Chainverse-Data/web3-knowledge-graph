import logging
from ..helpers import Ingestor
from .cyphers import TokenHoldersCyphers
import re
import pandas as pd
import sys
try:
    sys.set_int_max_str_digits(0) # Required for very large numbers!
except:
    pass

class TokenHoldersIngestor(Ingestor):
    def __init__(self, bucket_name="token-holders"):
        self.cyphers = TokenHoldersCyphers()
        super().__init__(bucket_name, load_data=False)

    def clean_symbol(self, symbol):
        if symbol:
            return re.sub(r'\W+', '', symbol)
        return ""

    def clean_decimal(self, decimal):
        if decimal:
            if type(decimal) == str and "0x" in decimal:
                return int(decimal, 16)
            return int(decimal)
        return -1

    def prepare_transfer_data(self):
        logging.info("Preparing transfer data")
        self.scraper_data["transfers"] = pd.DataFrame.from_dict(self.scraper_data["transfers"]).drop_duplicates(["from", "to", "hash"])
        from_wallets = pd.DataFrame(self.scraper_data["transfers"]["from"].unique(), columns=["address"])
        to_wallets = pd.DataFrame(self.scraper_data["transfers"]["to"].unique(), columns=["address"])
        transfer_wallets = from_wallets.append(to_wallets).drop_duplicates("address")
        return transfer_wallets

    def ingest_transfers(self):
        transfer_wallets = self.prepare_transfer_data()
        urls = self.save_df_as_csv(transfer_wallets, f"ingestor_transfers_wallets_{self.asOf}")
        self.cyphers.queries.create_wallets(urls)
        urls = self.save_df_as_csv(self.scraper_data["transfers"], f"ingestor_transfers_{self.asOf}")
        self.cyphers.link_or_merge_transfers(urls)

    def prepare_token_data(self):
        logging.info("Preparing token data")
        data = {
            "ERC20": [],
            "ERC721": [],
            "ERC1155": []
        }
        for tokenAddress in self.scraper_data["tokens"]:
            token = self.scraper_data["tokens"][tokenAddress]
            symbol = token["symbol"]
            decimal = token["decimal"]
            if type(symbol) == str:
                symbol.replace(",", "")
            if type(decimal) == str:
                decimal.replace(",", ".")

            tmp = {
                "contractAddress": tokenAddress, 
                "symbol": symbol,
                "decimal": decimal
            }
            data[token["contractType"].upper()].append(tmp)
        
        for token_type in data:
            data[token_type] = pd.DataFrame.from_dict(data[token_type])
            data[token_type]["symbol"] = data[token_type]["symbol"].apply(self.clean_symbol)
            data[token_type]["decimal"] = data[token_type]["decimal"].apply(self.clean_decimal)
        
        return data

    def ingest_tokens(self):
        logging.info("Ingesting token data")
        token_data = self.prepare_token_data()
        for tokenType in token_data:
            logging.info(f"Ingesting : {tokenType}")
            urls = self.save_df_as_csv(token_data[tokenType], f"ingestor_tokens_{tokenType}_{self.asOf}", max_lines=5000)
            self.cyphers.create_or_merge_tokens(urls, tokenType)

    def prepare_holdings_data(self):
        logging.info("Preparing balances data")
        data = []
        wallets = [wallet for wallet in self.scraper_data["balances"] if not self.is_zero_address(wallet)]
        for wallet in wallets:
            for balance in self.scraper_data["balances"][wallet]:
                if type(balance) == dict and "error" not in balance:
                    contractAddress = balance["contractAddress"]
                    if contractAddress in self.scraper_data["tokens"] and self.is_valid_address(contractAddress) and not self.is_zero_address(contractAddress):
                        decimal = self.scraper_data["tokens"][contractAddress]["decimal"]
                        if type(decimal) == str and "0x" in decimal:
                            decimal = int(decimal, 16)
                        elif type(decimal) == str:
                            decimal = int(decimal)
                        if balance["tokenBalance"] == "0x":
                            balance["tokenBalance"] = "0x0"
                        if self.scraper_data["tokens"][contractAddress]["contractType"] == "erc20" and decimal:
                            numericBalance = int(balance["tokenBalance"], 16) / 10**decimal
                        else:
                            numericBalance = int(balance["tokenBalance"], 16)
                            if numericBalance > 10**9:
                                numericBalance = numericBalance // 10**18
                        data.append({
                            "address": wallet,
                            "contractAddress": contractAddress,
                            "balance": balance["tokenBalance"],
                            "numericBalance": str(numericBalance)
                        })
        data = pd.DataFrame.from_dict(data)
        return data

    def ingest_holdings(self):
        logging.info("Ingesting balances data")
        holding_data = self.prepare_holdings_data()
        urls = self.save_df_as_csv(holding_data, f"ingestor_holdings_{self.asOf}", max_lines=5000)
        self.cyphers.link_wallet_tokens(urls)

    def run(self):
        for data in self.load_data_iterate():
            self.scraper_data = data
            self.ingest_tokens()
            self.ingest_holdings()
            if "transfers" in self.scraper_data:
                self.ingest_transfers()
            self.save_metadata()

if __name__ == '__main__':
    ingestor = TokenHoldersIngestor()
    ingestor.run()