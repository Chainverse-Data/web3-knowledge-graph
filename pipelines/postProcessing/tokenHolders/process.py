import argparse
from datetime import datetime, timedelta, timezone
import logging
from .cypher import TokenHoldersCyphers
from ..helpers import Processor
from ...helpers import Alchemy, Etherscan
import os

DEBUG = os.environ.get("DEBUG", False)
DEBUG = False

class TokenHoldersProcessor(Processor):
    def __init__(self, address, chain, tokenType, bucket_name="token-holders"):
        self.address = address
        self.chain = chain
        self.tokenType = tokenType
        self.cyphers = TokenHoldersCyphers()
        super().__init__(bucket_name)
        self.alchemy = Alchemy() 
        self.etherscan = Etherscan() 

    def get_holders_for_NFT_tokens(self):
        data = [self.alchemy.getOwnersForCollection(self.address)]
        results = []
        for token, holders in zip([self.address], data):
            for element in holders:
                for balance in element["tokenBalances"]:
                    tmp = {
                        "contractAddress": token,
                        "address": element["ownerAddress"],
                        "tokenId": balance["tokenId"],
                        "balance": balance["balance"]
                    }
                    results.append(tmp)
        self.cyphers.mark_current_hold_edges([self.address])
        urls = self.save_json_as_csv(results, f"process_nft_tokens_{self.asOf}_{self.address}")
        self.cyphers.queries.create_wallets(urls)
        self.cyphers.clean_NFT_token_holding(urls)
        self.cyphers.link_or_merge_NFT_token_holding(urls)
        self.cyphers.move_old_hold_edges_to_held([self.address])
        self.cyphers.update_tokens([self.address])
    
    def get_holders_for_ERC20_token(self):
        logging.info("Getting ERC20 token holders from Etherscan")
        data = self.etherscan.get_token_holders(self.address, chain=self.chain)
        logging.info("Getting ERC20 token metadata from Etherscan")
        metadata = self.etherscan.get_token_information(self.address, chain=self.chain)
        if metadata and data:
            results = []
            divisor = metadata["divisor"]
            for holder in data:
                numericBalance = None
                if divisor:
                    try:
                        numericBalance = int(holder["TokenHolderQuantity"]) / 10**int(divisor)
                    except:
                        pass
                tmp = {
                    "contractAddress": self.address,
                    "address": holder["TokenHolderAddress"],
                    "balance": holder["TokenHolderQuantity"],
                    "numericBalance": numericBalance
                }
                results.append(tmp)
            logging.info("Saving the data")
            self.cyphers.mark_current_hold_edges([self.address])
            urls = self.save_json_as_csv(results, f"process_erc20_tokens_{self.asOf}_{self.address}")
            self.cyphers.queries.create_wallets(urls)
            self.cyphers.link_or_merge_ERC20_token_holding(urls)
            self.cyphers.move_old_hold_edges_to_held([self.address])
            self.cyphers.update_tokens([self.address])

    def run(self):
        self.cyphers.set_pipeline_status(self.address)
        if self.tokenType == "ERC20":
            self.get_holders_for_ERC20_token()
        else:
            self.get_holders_for_NFT_tokens()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='TokenHolders arguments.')
    parser.add_argument('-t', '--tokenAddress', type=str, help='the token address')
    parser.add_argument('-e', '--erc', type=str, help='the token contract type: ERC20 | ERC721 | ERC1155')
    parser.add_argument('-c', '--chain' ,type=str, help='the chain the token is on: ethereum | polygon | arbitrum | optimism (ERC20 only: binance)')
    args = parser.parse_args()
    print(args)
    P = TokenHoldersProcessor(args.tokenAddress, args.chain, args.erc)
    P.run()