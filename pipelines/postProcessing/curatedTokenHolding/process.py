
from datetime import datetime, timedelta, timezone
import logging
from .cypher import CuratedTokenHoldingCyphers
from ..helpers import Processor
from ...helpers import Alchemy, Etherscan
import os

DEBUG = os.environ.get("DEBUG", False)
DEBUG = False

class CuratedTokenHoldingProcessor(Processor):
    def __init__(self, bucket_name="citizen-token-holding"):
        self.cyphers = CuratedTokenHoldingCyphers()
        super().__init__(bucket_name)
        self.NFT_chunk_size = 10
        self.ERC20_chunk_size = 10
        self.ERC20_last_block = self.metadata.get("ERC20_last_block", {})
        self.alchemy = Alchemy() 
        self.etherscan = Etherscan() 

    def get_NFTs_tokens(self) -> list[str]:
        tokens_data = self.cyphers.get_manual_selection_NFT_tokens()
        tokens = []
        for token in tokens_data:
            if token["schedule"] == "daily" and datetime.now().replace(tzinfo=timezone.utc) - timedelta(days=1) >= token["lastHoldersUpdateDt"].replace(tzinfo=timezone.utc):
                tokens.append(token["address"])
            if token["schedule"] == "weekly" and datetime.now().replace(tzinfo=timezone.utc) - timedelta(days=7) >= token["lastHoldersUpdateDt"].replace(tzinfo=timezone.utc):
                tokens.append(token["address"])
            if token["schedule"] == "monthly" and datetime.now().replace(tzinfo=timezone.utc) - timedelta(days=30) >= token["lastHoldersUpdateDt"].replace(tzinfo=timezone.utc):
                tokens.append(token["address"])
        if DEBUG:
            tokens = tokens[:10]
            return tokens
        logging.info(f"{len(tokens)} ERC721 and ERC1155 tokens retrieved for processing!")
        return tokens
    
    def get_ERC20_tokens(self) -> list[str]:
        tokens_data = self.cyphers.get_manual_selection_ERC20_tokens()
        tokens = []
        for token in tokens_data:
            if token["schedule"] == "daily" and datetime.now() - timedelta(days=1) >= token["lastHoldersUpdateDt"]:
                tokens.append(token["address"])
            if token["schedule"] == "weekly" and datetime.now() - timedelta(days=7) >= token["lastHoldersUpdateDt"]:
                tokens.append(token["address"])
            if token["schedule"] == "monthly" and datetime.now() - timedelta(days=30) >= token["lastHoldersUpdateDt"]:
                tokens.append(token["address"])
        if DEBUG:
            tokens = tokens[:10]
        logging.info(f"{len(tokens)} ERC20 tokens retrieved for processing!")
        return tokens

    def get_holders_for_NFT_tokens(self):
        tokens = self.get_NFTs_tokens()
        for i in range(0, len(tokens), self.NFT_chunk_size):
            current_tokens = tokens[i: i+self.NFT_chunk_size]
            data = self.parallel_process(self.alchemy.getOwnersForCollection, current_tokens, description="Getting NFT token Holders")
            results = []
            for token, holders in zip(current_tokens, data):
                for element in holders:
                    for balance in element["tokenBalances"]:
                        tmp = {
                            "contractAddress": token,
                            "address": element["ownerAddress"],
                            "tokenId": balance["tokenId"],
                            "balance": balance["balance"]
                        }
                        results.append(tmp)
            self.cyphers.mark_current_hold_edges(current_tokens)
            urls = self.save_json_as_csv(results, f"process_nft_tokens_{self.asOf}_{i}")
            self.cyphers.queries.create_wallets(urls)
            self.cyphers.clean_NFT_token_holding(urls)
            self.cyphers.link_or_merge_NFT_token_holding(urls)
            self.cyphers.move_old_hold_edges_to_held(current_tokens)
            self.cyphers.update_tokens(current_tokens)
    
    def get_holders_for_ERC20_token(self, token):
        data = self.etherscan.get_token_holders(token)
        metadata = self.etherscan.get_token_information(token)
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
                    "contractAddress": token,
                    "address": holder["TokenHolderAddress"],
                    "balance": holder["TokenHolderQuantity"],
                    "numericBalance": numericBalance
                }
                results.append(tmp)
            self.cyphers.mark_current_hold_edges([token])
            urls = self.save_json_as_csv(results, f"process_erc20_tokens_{self.asOf}_{token}")
            self.cyphers.queries.create_wallets(urls)
            self.cyphers.link_or_merge_ERC20_token_holding(urls)
            self.cyphers.move_old_hold_edges_to_held([token])
            self.cyphers.update_tokens([token])

    def get_holders_for_ERC20_tokens(self):
        tokens = self.get_ERC20_tokens()
        self.parallel_process(self.get_holders_for_ERC20_token, tokens, "Getting holders for ERC20 tokens")

    def run(self):
        self.get_holders_for_NFT_tokens()
        self.get_holders_for_ERC20_tokens()

if __name__ == "__main__":
    P = CuratedTokenHoldingProcessor()
    P.run()