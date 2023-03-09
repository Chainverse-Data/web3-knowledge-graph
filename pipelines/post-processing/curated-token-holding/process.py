
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
        self.NFT_chunk_size = 100
        self.ERC20_chunk_size = 100
        self.ERC20_last_block = self.metadata.get("ERC20_last_block", {})
        self.alchemy = Alchemy() 
        self.etherscan = Etherscan() 

    def get_NFTs_tokens(self):
        tokens = self.cyphers.get_bluechip_NFT_tokens(min_price=10)
        tokens += self.cyphers.get_citizen_NFT_tokens(propotion=0.25)
        tokens += self.cyphers.get_overrepresented_NFT_tokens(propotion=0.01)
        tokens += self.cyphers.get_manual_selection_NFT_tokens()
        if DEBUG:
            tokens = tokens[:10]
            return tokens
        logging.info(f"{len(tokens)} tokens retrieved for processing!")
        return tokens
    
    def get_ERC20_tokens(self):
        tokens = self.cyphers.get_citizen_ERC20_tokens(propotion=0.25)
        if DEBUG:
            tokens = tokens[:10]
        return tokens

    def get_holders_for_NFT_tokens(self):
        tokens = self.get_NFTs_tokens()
        for i in range(0, len(tokens), self.NFT_chunk_size):
            data = self.parallel_process(self.alchemy.getOwnersForCollection, tokens[i: i+self.NFT_chunk_size], description="Getting NFT token Holders")
            results = []
            for token, holders in zip(tokens[i: i+self.NFT_chunk_size], data):
                for element in holders:
                    for balance in element["tokenBalances"]:
                        tmp = {
                            "contractAddress": token,
                            "address": element["ownerAddress"],
                            "tokenId": balance["tokenId"],
                            "balance": balance["balance"]
                        }
                        results.append(tmp)
            urls = self.save_json_as_csv(results, self.bucket_name, f"process_nft_tokens_{self.asOf}_{i}")
            self.cyphers.link_or_merge_NFT_token_holding(urls)
    
    def get_holders_for_ERC20_tokens(self):
        tokens = self.get_ERC20_tokens()
        
        # for i in range(0, len(tokens)):
        #     data = self.alchemy.getAssetTransfers([tokens[i]])
        #     results = []
        #     for token, holders in zip(tokens, data):
        #         for balance in holders["tokenBalances"]:
        #             tmp = {
        #                 "contractAddress": token,
        #                 "address": holders["ownerAddress"],
        #                 "tokenId": balance["tokenId"],
        #                 "balance": balance["balance"]
        #             }
        #             results.append(tmp)
        #     urls = self.save_json_as_csv(results, self.bucket_name, f"process_nft_tokens_{self.asOf}_{i}")
        #     self.cyphers.link_or_merge_NFT_token_holding(urls)

    def run(self):
        self.get_holders_for_NFT_tokens()
        # self.get_holders_for_ERC20_tokens()

if __name__ == "__main__":
    P = CuratedTokenHoldingProcessor()
    P.run()